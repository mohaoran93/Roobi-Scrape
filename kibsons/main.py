import time
import sys
import os
from elasticsearch import NotFoundError, helpers
import logging
import h3
from datetime import datetime
from kibsons.call_xbytes import get_categories, get_all_products
import json
import argparse

# from tools import send_telegram_message
from functions import (
    get_esinstance,
    kibsons_index_name_product,
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
logger = logging.getLogger("Kibsons-Scrapy")

es = get_esinstance()

def run_kibsons(clear_existing=False, recreate_index=False):
    total_products = 0
    total_categories_processed = 0
    t1 = time.time()
    
    # Handle index management
    if recreate_index:
        logger.info("Recreating index...")
        delete_index(es, kibsons_index_name_product)
        create_index_if_not_exists(es, kibsons_index_name_product)
    elif clear_existing:
        logger.info("Clearing existing data...")
        clear_index(es, kibsons_index_name_product)
    else:
        create_index_if_not_exists(es, kibsons_index_name_product)
    
    current_time_iso = datetime.utcnow()
    current_time = current_time_iso.isoformat()

    logger.info(f"Using index: {kibsons_index_name_product}")
    
    # Step 1: Get categories
    try:
        get_categories_res = get_categories()
        os.makedirs("temp", exist_ok=True)
        with open(f'temp/categories_data.json', 'w') as file:
            json.dump(get_categories_res, file, indent=4)
        
        logger.info(f"Categories response status: {get_categories_res.get('status', 'unknown')}")
        categories = get_categories_res.get("Categories", [])
        
        if not categories:
            logger.error("No categories found")
            return
            
        logger.info(f"Found {len(categories)} categories")
        
    except Exception as e:
        logger.error(f"Error getting categories: {e}")
        return

    # Step 2: Process each category
    for category in categories:
        try:
            logger.info(f"Processing category: {category}")
            products_data = get_all_products(category)
            
            if products_data is None:
                logger.warning(f"No products data returned for category {category}")
                continue
            
            if not isinstance(products_data, dict):
                logger.warning(f"Invalid products data format for category {category}")
                continue
                
            total_categories_processed += 1
            
            # Process products for this category
            categories_key = products_data.keys()
            for category_key in categories_key:
                products = products_data.get(category_key, [])
                
                if not products:
                    logger.info(f"No products found for category key: {category_key}")
                    continue
                    
                logger.info(f"Processing {len(products)} products under {category_key}")
                total_products += len(products)
                
                actions = []
                processed_products = 0
                
                for product in products:
                    try:
                        product_to_es = {}
                        
                        # Extract and validate product data
                        product_to_es["product_name_orig"] = product.get("Title", "")
                        
                        # Handle price with fallbacks
                        price = product.get("Price", product.get("Discount Price"))
                        if not price or price == "" or price == "0" or price == 0:
                            price = product.get("Actual Price")
                        if not price or price == "" or price == "0" or price == 0:
                            logger.debug(f"Skipping product {product.get('Title', 'unknown')} - no valid price found")
                            continue
                        
                        # Convert price to float
                        try:
                            product_to_es["price"] = float(price)
                        except (ValueError, TypeError):
                            logger.debug(f"Invalid price format for product {product.get('Title', 'unknown')}: {price}")
                            continue
                        
                        # Extract other fields
                        product_to_es["price_per_size"] = product.get("Price Per Size", "")
                        product_to_es["product_id"] = product.get("Product ID")
                        product_to_es["product_code"] = product.get("Product Code")
                        product_to_es["description"] = product.get("Description", "")
                        
                        # Handle popularity score
                        try:
                            product_to_es["popularity_score"] = float(product.get("Popularity Score", 0))
                        except (ValueError, TypeError):
                            product_to_es["popularity_score"] = 0.0
                        
                        product_to_es["origin_country"] = product.get("Origin Country", "")
                        product_to_es["instock"] = str(product.get("InStock", ""))
                        product_to_es["product_image"] = product.get("ImageURL", "")
                        product_to_es["category"] = category
                        product_to_es["product_brand"] = product.get("Brand", "")
                        
                        # Validate required fields
                        if not product_to_es["product_id"] or not product_to_es["product_code"]:
                            logger.debug(f"Skipping product {product.get('Title', 'unknown')} - missing ID or code")
                            continue
                        
                        # Create document ID
                        doc_id = f"{product_to_es['product_id']}_{product_to_es['product_code']}_test"
                        
                        actions.append({
                            "_index": kibsons_index_name_product,
                            "_id": doc_id,
                            "_source": {
                                **product_to_es,
                                "last_scraped_time": current_time,
                                "test_run": True,
                                "platform": "kibsons"
                            }
                        })
                        processed_products += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing product {product.get('Title', 'unknown')}: {e}")
                        continue
                
                # Bulk index products
                if actions:
                    try:
                        helpers.bulk(es, actions, raise_on_error=True)
                        logger.info(f"Successfully indexed {len(actions)} products for category {category_key}")
                    except Exception as e:
                        logger.error(f"Error bulk indexing products for category {category_key}: {e}")
                        
        except Exception as e:
            logger.error(f"Error processing category {category}: {e}")
            continue
    
    t2 = time.time()
    duration = int(t2 - t1)
    
    logger.info(f"Scraping completed in {duration} seconds")
    logger.info(f"Categories processed: {total_categories_processed}")
    logger.info(f"Total products scraped: {total_products}")
    
    # send_telegram_message(f"Kibsons scraping completed\nDuration: {duration}s\nCategories: {total_categories_processed}\nTotal products: {total_products}\nIndex: {kibsons_index_name_product}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kibsons Scraper')
    parser.add_argument('--clear', action='store_true', help='Clear existing data before scraping (keeps index structure)')
    parser.add_argument('--recreate', action='store_true', help='Delete and recreate the index completely')
    
    args = parser.parse_args()
    
    logger.info("Starting Kibsons scraper")
    
    if args.recreate:
        logger.info("Will recreate the index (delete and create new)")
    elif args.clear:
        logger.info("Will clear existing data (keep index structure)")
    else:
        logger.info("Will use existing index or create if not exists")
    
    run_kibsons(clear_existing=args.clear, recreate_index=args.recreate)