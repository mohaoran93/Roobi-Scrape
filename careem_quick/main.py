import time
import os
from elasticsearch import NotFoundError, helpers
import logging
import h3
from datetime import datetime
from careem_quick.call_xbytes import listing_categories, listing_products, list_all_products
import json
import argparse

# from tools import send_telegram_message
from functions import (
    get_esinstance, 
    careem_quick_index_name_product, 
    CACHED_TIME_THRESHOLD,
    create_index_if_not_exists,
    clear_index,
    delete_index
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("CareemQuick-Scrapy")

es = get_esinstance()

def run_careem_quick(latitude, longitude, resolution=10, clear_existing=False, recreate_index=False):
    """
    Run Careem Quick scraper
    
    Args:
        latitude: Location latitude
        longitude: Location longitude  
        resolution: H3 resolution
        clear_existing: If True, clear existing data before scraping
        recreate_index: If True, delete and recreate the index
    """
    total_products = 0
    product_ids = set()
    t1 = time.time()
    
    # Handle index management
    if recreate_index:
        logger.info("Recreating index...")
        delete_index(es, careem_quick_index_name_product)
        create_index_if_not_exists(es, careem_quick_index_name_product)
    elif clear_existing:
        logger.info("Clearing existing data...")
        clear_index(es, careem_quick_index_name_product)
    else:
        create_index_if_not_exists(es, careem_quick_index_name_product)
    
    h3_cell = h3.geo_to_h3(latitude, longitude, resolution)
    geo = {"lat": latitude, "lon": longitude}
    logger.info("received geo location {}".format(geo))
    logger.info(f"Using index: {careem_quick_index_name_product}")
    
    # Step 1: Get all categories
    try:
        categories_res = listing_categories(latitude, longitude)
        os.makedirs("temp", exist_ok=True)
        with open(f'temp/categories_data_{h3_cell}.json', 'w') as file:
            json.dump(categories_res, file, indent=4)
        
        logger.info(f"Categories response status: {categories_res.get('status', 'unknown')}")
        
        # Improved error handling for categories
        if not categories_res or "results" not in categories_res:
            logger.error("Invalid categories response structure")
            return
            
        results = categories_res.get("results", {})
        if not results or "data" not in results:
            logger.error("No data in categories response")
            return
            
        data = results.get("data", {})
        categories = data.get("category", [])
        zone_id = data.get("zone_id")
        
        if not categories:
            logger.error("No categories found")
            return
            
        if not zone_id:
            logger.error("No zone_id found")
            return
            
        logger.info(f"Found {len(categories)} categories for zone_id: {zone_id}")
        
    except Exception as e:
        logger.error(f"Error getting categories: {e}")
        return
    
    current_time_iso = datetime.utcnow()
    current_time = current_time_iso.isoformat()
    
    # Step 2: Process each category and subcategory
    for category in categories:
        category_name = category.get("category_name", "unknown")
        category_id = category.get("category_id", "unknown")
        logger.info(f"Processing category: {category_name} (ID: {category_id})")
        
        subcategories = category.get("subcategory_list", [])
        if not subcategories:
            logger.warning(f"No subcategories found for category: {category_name}")
            continue
            
        for sub_category in subcategories:
            subcategory_id = sub_category.get("subcategory_id")
            subcategory_name = sub_category.get("subcategory_name", "unknown")
            
            if not subcategory_id:
                logger.warning(f"No subcategory_id found for: {subcategory_name}")
                continue
                
            logger.info(f"Processing subcategory: {subcategory_name} (ID: {subcategory_id})")
            
            try:
                all_products = list_all_products(latitude, longitude, zone_id, subcategory_id)
                
                if not all_products or len(all_products) == 0:
                    logger.warning(f"No products found for subcategory: {subcategory_name}")
                    continue
                    
                logger.info(f"Found {len(all_products)} products for subcategory: {subcategory_name}")
                total_products += len(all_products)
                product_ids.update([product.get("product_id") for product in all_products if product.get("product_id")])
                
                # Prepare bulk actions
                actions = []
                for product in all_products:
                    product_id = product.get("product_id")
                    if not product_id:
                        continue
                        
                    # Add metadata
                    product_data = {
                        **product,
                        "last_scraped_time": current_time,
                        "subcategory_id": subcategory_id,
                        "subcategory_name": subcategory_name,
                        "category_id": category_id,
                        "category_name": category_name,
                        "zone_id": zone_id,
                        "h3_cell": h3_cell,
                        "test_run": True
                    }
                    
                    actions.append({
                        "_index": careem_quick_index_name_product,
                        "_id": f"careem_quick_test_{product_id}",
                        "_source": product_data
                    })
                
                # Bulk index to Elasticsearch
                if actions:
                    helpers.bulk(es, actions, raise_on_error=True)
                    logger.info(f"Indexed {len(actions)} products for subcategory: {subcategory_name}")
                    
            except Exception as e:
                logger.error(f"Error processing subcategory {subcategory_name}: {e}")
                continue
    
    t2 = time.time()
    duration = int(t2 - t1)
    
    logger.info(f"Scraping completed in {duration} seconds")
    logger.info(f"Total products scraped: {total_products}")
    logger.info(f"Unique product IDs: {len(product_ids)}")
    
    # send_telegram_message(f"Careem Quick TEST scraping completed\n"
    #                      f"Duration: {duration}s\n"
    #                      f"Total products: {total_products}\n"
    #                      f"Unique products: {len(product_ids)}\n"
    #                      f"Index: {careem_quick_index_name_product}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Careem Quick Scraper')
    parser.add_argument('geoloc', help='Latitude,Longitude (e.g., "25.042983969226388,55.23489620876715")')
    parser.add_argument('--clear', action='store_true', help='Clear existing data before scraping')
    parser.add_argument('--recreate', action='store_true', help='Delete and recreate the index')
    
    args = parser.parse_args()
    
    latitude, longitude = args.geoloc.split(",")
    latitude = float(latitude.strip())
    longitude = float(longitude.strip())
    
    logger.info(f"Starting scraper with coordinates: {latitude}, {longitude}")
    
    if args.recreate:
        logger.info("Will recreate the index")
    elif args.clear:
        logger.info("Will clear existing data")
    
    run_careem_quick(latitude, longitude, clear_existing=args.clear, recreate_index=args.recreate)