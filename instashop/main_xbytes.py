import time
import sys
import os
from elasticsearch import NotFoundError, helpers
import logging
import h3
from datetime import datetime
from instashop.call_x_bytes import instashop_list_stores_all_categories, instashop_store_details, instashop_list_store_products
import json
import argparse

# from tools import send_telegram_message
from functions import (
    get_esinstance,
    index_name_store,
    index_name_product,
    CACHED_TIME_THRESHOLD,
    instashop_store_mapping,
    instashop_product_mapping,
    create_index_if_not_exists_with_mapping,
    clear_index,
    delete_index
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("Instashop-Scrapy")

es = get_esinstance()

def run_instashop(latitude, longitude, resolution=10, clear_existing=False, recreate_index=False):
    t1 = time.time()
    number_of_cached_store = 0
    total_products_scraped = 0
    
    # Handle index management
    if recreate_index:
        logger.info("Recreating indices...")
        delete_index(es, index_name_store)
        delete_index(es, index_name_product)
        create_index_if_not_exists_with_mapping(es, index_name_store, instashop_store_mapping)
        create_index_if_not_exists_with_mapping(es, index_name_product, instashop_product_mapping)
    elif clear_existing:
        logger.info("Clearing existing data...")
        clear_index(es, index_name_store)
        clear_index(es, index_name_product)
    else:
        create_index_if_not_exists_with_mapping(es, index_name_store, instashop_store_mapping)
        create_index_if_not_exists_with_mapping(es, index_name_product, instashop_product_mapping)
    
    h3_cell = h3.geo_to_h3(latitude, longitude, resolution)
    geo = {"lat": latitude, "lon": longitude}
    logger.info(f"Received geo location: {geo}")
    logger.info(f"Using indices - Store: {index_name_store}, Product: {index_name_product}")
    
    # Step 1: Get all stores
    get_all_store = True
    os.makedirs("temp", exist_ok=True)
    
    if get_all_store:
        try:
            stores_res, storeid2category = instashop_list_stores_all_categories(latitude, longitude)
            stores = stores_res
            
            if not stores:
                logger.error("No stores found")
                # send_telegram_message(f"Instashop scraping warning: No stores found at {latitude}, {longitude}")
                return
                
            logger.info(f"Found {len(stores)} stores")
            
            with open(f'temp/store_data_{h3_cell}.json', 'w') as file:
                json.dump(stores, file, indent=4)
                
        except Exception as e:
            logger.error(f"Error getting stores: {e}")
            return
    else:
        logger.warning("Loading from cache - set get_all_store = True for production")
        with open(f'temp/store_data_{h3_cell}.json') as f:
            stores = json.load(f)
            storeid2category = {}
    
    # Step 2: Process each store
    current_time_iso = datetime.utcnow()
    current_time = current_time_iso.isoformat()
    
    for store in stores:
        store_id = store.get("store_id")
        if not store_id:
            logger.warning("Store without store_id, skipping")
            continue
            
        logger.info(f"Processing store: {store_id}")
        
        # Check if store was recently scraped
        try:
            if es.indices.exists(index=index_name_store):
                query_store = {
                    "query": {"term": {"store_id": store_id}},
                    "sort": [{"last_scraped_time": {"order": "desc"}}]
                }
                store_info_hits = es.search(index=index_name_store, **query_store)["hits"]["hits"]
                
                if store_info_hits:
                    store_info = store_info_hits[0]["_source"]
                    last_scraped_time = datetime.fromisoformat(store_info.get("last_scraped_time"))
                else:
                    last_scraped_time = datetime.fromisoformat("1990-01-01T00:00:00")
            else:
                last_scraped_time = datetime.fromisoformat("1990-01-01T00:00:00")
                
        except Exception as e:
            logger.warning(f"Error checking store cache: {e}")
            last_scraped_time = datetime.fromisoformat("1990-01-01T00:00:00")
        
        # Step 3: Scrape store if not recently scraped
        if (current_time_iso - last_scraped_time).total_seconds() > int(CACHED_TIME_THRESHOLD):
            try:
                category_name = storeid2category.get(store_id, "")
                all_categories = instashop_store_details(
                    store_id=store_id,
                    latitude=latitude,
                    longitude=longitude,
                    category_name=category_name
                )
                
                if not all_categories:
                    logger.warning(f"No categories found for store: {store_id}")
                    continue
                
                logger.info(f"Found {len(all_categories)} categories for store: {store_id}")
                
                for category in all_categories:
                    category_id = category.get("category_id")
                    if not category_id:
                        continue
                        
                    logger.debug(f"Processing category {category_id} for store {store_id}")
                    
                    try:
                        products = instashop_list_store_products(
                            store_id=store_id,
                            category_id=category_id
                        )
                        
                        if not products:
                            logger.debug(f"No products found for store {store_id}, category {category_id}")
                            continue
                        
                        logger.info(f"Found {len(products)} products for store {store_id}, category {category_id}")
                        
                        # Prepare bulk actions for products
                        actions = []
                        for product in products:
                            product_id = product.get("product_id")
                            if not product_id:
                                continue
                                
                            # Clean product data
                            product.pop("product_price_all", None)
                            
                            product_data = {
                                **product,
                                "last_scraped_time": current_time,
                                "h3_cell": h3_cell,
                                "test_run": True,
                                "platform": "instashop"
                            }
                            
                            actions.append({
                                "_index": index_name_product,
                                "_id": f"{store_id}_{product_id}_test",
                                "_source": product_data
                            })
                        
                        if actions:
                            helpers.bulk(es, actions, raise_on_error=True)
                            total_products_scraped += len(actions)
                            logger.info(f"Indexed {len(actions)} products for store {store_id}")
                            
                    except Exception as e:
                        logger.error(f"Error processing category {category_id} for store {store_id}: {e}")
                        continue
                
                # Index store information
                store_data = {
                    **store,
                    "h3_cell": h3_cell,
                    "geo": geo,
                    "last_scraped_time": current_time,
                    "test_run": True,
                    "platform": "instashop"
                }
                
                es.index(
                    index=index_name_store,
                    id=f"{store_id}_{h3_cell}_test",
                    document=store_data
                )
                
            except Exception as e:
                logger.error(f"Error processing store {store_id}: {e}")
                continue
        else:
            number_of_cached_store += 1
            logger.info(f"Store {store_id} recently scraped, skipping")
            
            # Still index the store info
            store_data = {
                **store,
                "h3_cell": h3_cell,
                "geo": geo,
                "last_scraped_time": current_time,
                "test_run": True,
                "platform": "instashop"
            }
            
            es.index(
                index=index_name_store,
                id=f"{store_id}_{h3_cell}_test",
                document=store_data
            )
    
    t2 = time.time()
    duration = int(t2 - t1)
    
    logger.info(f"Scraping completed in {duration} seconds")
    logger.info(f"Processed {len(stores)} stores, {number_of_cached_store} from cache")
    logger.info(f"Total products scraped: {total_products_scraped}")
    
    # send_telegram_message(f"Instashop scraping completed\nDuration: {duration}s\nStores: {len(stores)}, {number_of_cached_store} from cache\nTotal products: {total_products_scraped}\nIndices - Store: {index_name_store}, Product: {index_name_product}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Instashop Scraper')
    parser.add_argument('geoloc', help='Latitude,Longitude (e.g., "25.042983969226388,55.23489620876715")')
    parser.add_argument('--clear', action='store_true', help='Clear existing data before scraping')
    parser.add_argument('--recreate', action='store_true', help='Delete and recreate the indices')
    
    args = parser.parse_args()
    
    latitude, longitude = args.geoloc.split(",")
    latitude = float(latitude.strip())
    longitude = float(longitude.strip())
    
    logger.info(f"Starting scraper with coordinates: {latitude}, {longitude}")
    
    if args.recreate:
        logger.info("Will recreate the indices")
    elif args.clear:
        logger.info("Will clear existing data")
    
    run_instashop(latitude, longitude, clear_existing=args.clear, recreate_index=args.recreate)