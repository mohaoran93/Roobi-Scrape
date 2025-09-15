import time
from elasticsearch import Elasticsearch, NotFoundError, helpers
from dotenv import dotenv_values
import logging
import h3
from datetime import datetime
import json
import os
import argparse

from nownow.call_x_bytes import nownow_list_stores, nownow_store_details, nownow_produt_details, list_store_products
from functions import (
    nownow_store_mapping, 
    nownow_product_mapping,
    get_esinstance, 
    index_name_store, 
    index_name_product, 
    CACHED_TIME_THRESHOLD,
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

logger = logging.getLogger("Nownow-Scrapy")

es = get_esinstance()

def run_nownow(latitude, longitude, resolution=10, clear_existing=False, recreate_index=False):
    """
    Run NowNow scraper
    
    Args:
        latitude: Location latitude
        longitude: Location longitude  
        resolution: H3 resolution
        clear_existing: If True, clear existing data before scraping
        recreate_index: If True, delete and recreate the index
    """
    t1 = time.time()
    number_of_cached_store = 0
    total_products_scraped = 0
    h3_cell = h3.geo_to_h3(latitude, longitude, resolution)
    geo = {"lat": latitude, "lon": longitude}
    
    # Handle index management
    if recreate_index:
        logger.info("Recreating indices...")
        delete_index(es, index_name_store)
        delete_index(es, index_name_product)
        create_index_if_not_exists_with_mapping(es, index_name_store, nownow_store_mapping)
        create_index_if_not_exists_with_mapping(es, index_name_product, nownow_product_mapping)
    elif clear_existing:
        logger.info("Clearing existing data...")
        clear_index(es, index_name_store)
        clear_index(es, index_name_product)
    else:
        create_index_if_not_exists_with_mapping(es, index_name_store, nownow_store_mapping)
        create_index_if_not_exists_with_mapping(es, index_name_product, nownow_product_mapping)
    
    logger.info(f"Using indices - Store: {index_name_store}, Product: {index_name_product}")
    
    # Step 1: Get all stores
    os.makedirs("temp", exist_ok=True)
    get_all_store = True
    if get_all_store:
        try:
            stores = nownow_list_stores(latitude, longitude)
            logger.info(f"Retrieved stores response: {stores}")
            
            # Check if stores data is valid
            if not stores or "results" not in stores:
                logger.error("Invalid stores response structure")
                return
                
            stores_data = stores.get("results", {}).get("data", {})
            if not stores_data or not stores_data.get("stores"):
                logger.error("No stores found in response")
                # send_telegram_message(f"NowNow scraping warning: No stores found. Response: {stores}")
                return
                
            filename = f'temp/store_data_{h3_cell}.json'
            with open(filename, 'w') as file:
                json.dump(stores, file, indent=4)
                
        except Exception as e:
            logger.error(f"Error getting stores: {e}")
            return
    else:
        # Read JSON from store_data.json
        with open('store_data.json') as f:
            stores = json.load(f)

    # Step 2: Get store details and products
    stores_details = stores.get("results", {}).get("data", {}).get("stores", [])
    logger.info(f"Processing {len(stores_details)} stores")
    
    for store in stores_details:
        current_time_iso = datetime.utcnow()
        current_time = current_time_iso.isoformat()
        
        store_code = store.get("Store_Code")
        if not store_code:
            logger.warning("Store without Store_Code, skipping")
            continue
            
        logger.info(f"Processing store: {store_code}")

        # Check if store exists and is recently scraped
        index_exists = es.indices.exists(index=index_name_store)
        
        if index_exists:
            try:
                query_store = {
                    "query": {"term": {"Store_Code.keyword": store_code}},
                    "sort": [{"last_scraped_time": {"order": "desc"}}]
                }
                store_info_hits = es.search(index=index_name_store, **query_store)["hits"]["hits"]
                
                if len(store_info_hits) > 0:
                    store_info = store_info_hits[0]["_source"]
                    last_scraped_time = datetime.fromisoformat(store_info.get("last_scraped_time"))
                else:
                    last_scraped_time = datetime.fromisoformat("1990-01-01T00:00:00")
                    store_info = None
                    
            except Exception as e:
                logger.warning(f"Error checking store cache: {e}")
                last_scraped_time = datetime.fromisoformat("1990-01-01T00:00:00")
                store_info = None
        else:
            last_scraped_time = datetime.fromisoformat("1990-01-01T00:00:00")
            
        # Step 3: Scrape each store categories if not recently scraped
        if (current_time_iso - last_scraped_time).total_seconds() > int(CACHED_TIME_THRESHOLD):
            try:
                store_details_res = nownow_store_details(store_code, latitude, longitude)
                
                if not store_details_res:
                    logger.error(f"Store details not found for {store_code}")
                    continue
                    
                categories = store_details_res.get("results", {}).get("data", {}).get("store_details", {}).get("Categories", [])
                
                if not categories:
                    logger.warning(f"No categories found for {store_code}")
                    logger.warning(f"Store details response: {store_details_res}")
                    continue
                    
                logger.info(f"Found {len(categories)} categories for store {store_code}")
                store_products_count = {}
                
                for category in categories:
                    category_code = category.get("Category_code")
                    if not category_code:
                        continue
                        
                    try:
                        products = list_store_products(store_code, category_code)
                        products_info = products.get("results", {}).get("data", {}).get("products", [])
                        
                        if not products_info:
                            logger.warning(f"No products found for {store_code} category {category_code}")
                            continue
                            
                        # Prepare bulk actions for products
                        actions = []
                        for product in products_info:
                            product_code = product.get("Product_Code")
                            if not product_code:
                                continue
                                
                            product_data = {
                                **product,
                                "last_scraped_time": current_time,
                                "h3_cell": h3_cell,
                                "platform": "nownow",
                                "test_run": True,
                                "Store_Code": store_code
                            }
                            
                            actions.append({
                                "_index": index_name_product,
                                "_id": f"{product_code}_test",
                                "_source": product_data
                            })
                        
                        if actions:
                            helpers.bulk(es, actions, raise_on_error=True)
                            store_products_count[category_code] = len(products_info)
                            total_products_scraped += len(products_info)
                            logger.info(f"Indexed {len(actions)} products for {store_code} category {category_code}")
                            
                    except Exception as e:
                        logger.error(f"Error processing category {category_code} for store {store_code}: {e}")
                        continue
                
                # Index store information
                store_data = {
                    **store,
                    "h3_cell": h3_cell,
                    "geo": geo,
                    "last_scraped_time": current_time,
                    "test_run": True
                }
                
                es.index(
                    index=index_name_store,
                    id=f"{store_code}_{h3_cell}_test",
                    document=store_data
                )
                
                logger.info(f"Indexed store {store_code} with {sum(store_products_count.values())} products")
                
            except Exception as e:
                logger.error(f"Error processing store {store_code}: {e}")
                continue
        else:
            number_of_cached_store += 1
            logger.info(f"Store {store_code} recently scraped, skipping")
            
            # Still index the store info
            store_data = {
                **store,
                "h3_cell": h3_cell,
                "geo": geo,
                "last_scraped_time": current_time,
                "test_run": True
            }
            
            es.index(
                index=index_name_store,
                id=f"{store_code}_{h3_cell}_test",
                document=store_data
            )
    
    t2 = time.time()
    duration = int(t2 - t1)
    
    logger.info(f"NowNow scraping completed in {duration} seconds")
    logger.info(f"Processed {len(stores_details)} stores, {number_of_cached_store} from cache")
    logger.info(f"Total products scraped: {total_products_scraped}")
    
    # send_telegram_message(f"NowNow scraping completed\n"
    #                      f"Duration: {duration}s\n"
    #                      f"Stores: {len(stores_details)}, {number_of_cached_store} from cache\n"
    #                      f"Total products: {total_products_scraped}\n"
    #                      f"Indices - Store: {index_name_store}, Product: {index_name_product}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='NowNow Scraper')
    parser.add_argument('geoloc', help='Latitude,Longitude (e.g., "25.042983969226388,55.23489620876715")')
    parser.add_argument('--clear', action='store_true', help='Clear existing data before scraping')
    parser.add_argument('--recreate', action='store_true', help='Delete and recreate the indices')
    
    args = parser.parse_args()
    
    latitude, longitude = args.geoloc.split(",")
    latitude = float(latitude.strip())
    longitude = float(longitude.strip())
    
    logger.info(f"Starting NowNow scraper with coordinates: {latitude}, {longitude}")
    
    if args.recreate:
        logger.info("Will recreate the indices")
    elif args.clear:
        logger.info("Will clear existing data")
    
    run_nownow(latitude, longitude, clear_existing=args.clear, recreate_index=args.recreate)