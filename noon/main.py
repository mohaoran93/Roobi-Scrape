"""
platform,
product_brand,
product_name_orig,
"store_name": "EROS",
"store_id": 660096,
"shop_logo": "https://images.deliveryhero.io/image/talabat/restaurants/Logo_white638116340545494910.jpg",
"delivery_time": "33 mins",
"min_order": 20,
"category": "Accessories & Cables",
"vendor_id": "67cf223f-7920-48a8-adc7-846ee218d6ec",
"bid": 699038,
"shop_type": 1,
"product_image": "https://images.deliveryhero.io/image/talabat-nv/SSC_PIM_1/194644115739.png",
"sub_category": "Cables and Connectors",
"product_id_orig": "ea7c89f1-f3e4-4343-8e92-d41c17303976",
"product_name": "Anker 322 USB-C To USB-C Cable - White Color, 60 Watts, 1.8m",
"product_original_price": 39,
"product_sale_price": 39,
"last_scraped_time": "2024-08-24T05:42:43.872444"
"""

from noon.call_xbytes import get_categories, get_products, get_all_products_with_pagenation
import time
import sys
import os
from elasticsearch import NotFoundError
import logging
from datetime import datetime
from elasticsearch import helpers
import json
import argparse

# from tools import send_telegram_message
from functions import (
    get_esinstance,
    index_name_product_noon,
    CACHED_TIME_THRESHOLD,
    create_index_if_not_exists,
    clear_index,
    delete_index
)

logging.basicConfig(level=logging.INFO,  # Set the minimum logging level
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log format
                    datefmt='%Y-%m-%d %H:%M:%S',  # Date format
                    handlers=[
                        # logging.FileHandler('app.log'),  # Log to a file
                        logging.StreamHandler()  # Also log to the console
                    ])
logger = logging.getLogger("Noon-Scrapy")

es = get_esinstance()

def run_noon(location="dubai", clear_existing=False, recreate_index=False):
    exclude_categories = ["Mobiles & accessories","Electronics","Gaming essentials","Beauty","Laptops & accessories"
                          "Small Appliances","Toys & Games","Baby gear","Large Appliances","Health & nutrition","Fragrance","Music","Furniture","Men's fashion","Women's fashion"
                          "Kids' fashion","Watches","Eyewear","Sports & outdoors","Automotive","Stationery & office supplies","Books","Mahali",
                          "Gift Cards"]
    
    # Handle index management
    if recreate_index:
        logger.info("Recreating index...")
        delete_index(es, index_name_product_noon)
        create_index_if_not_exists(es, index_name_product_noon)
    elif clear_existing:
        logger.info("Clearing existing data...")
        clear_index(es, index_name_product_noon)
    else:
        create_index_if_not_exists(es, index_name_product_noon)
    
    os.makedirs("temp", exist_ok=True)
    USE_CACHE = False
    total_products_scraped = 0
    current_time_iso = datetime.utcnow()
    current_time = current_time_iso.isoformat()
    t1 = time.time()
    
    logger.info(f"Using index: {index_name_product_noon}")
    
    if USE_CACHE == False:
        categories = get_categories(location="dubai")
        with open(f'temp/categories_data_{location}.json', 'w') as file:
            json.dump(categories, file, indent=4)
    else:
        with open(f'temp/categories_data_{location}.json') as f:
            categories = json.load(f)
    if categories.get("status") != 200:
        # send_telegram_message(f"Failed to get categories from noon {categories}")
        logger.error(categories)
        return
    processed_categories = []
    category_data = categories.get("category_data", {})
    for category_level1 in category_data.keys():
        if category_level1 in exclude_categories or category_level1 in processed_categories:
            logger.info(f"skip {category_level1}")
            continue
        logger.info(f"working on category_level1 {category_level1}")
        slugs = category_data.get(category_level1,{}).get("slugs",[])
        if not slugs:
            logger.warning(f"No slugs found for category {category_level1}")
            continue
        slugs_shortest = min(slugs, key=len)
        category = slugs_shortest.replace("/","")
        logger.info(f"working on category {category}")
        products_res = get_all_products_with_pagenation(location=location,store_name=category)
        # logger.info(f"products_res {products_res}")
        if type(products_res) == list and len(products_res) == 0:
            logger.info(f"no products found for {category}")
            continue
        logger.info(f"total products {len(products_res)} under {category}")
        total_products = len(products_res)
        total_products_scraped += total_products
        actions = []
        with open(f"temp/noon_products_sample_{category}.json","w") as f:
            json.dump(products_res,f,indent=4)
        for product in products_res:
            logger.debug(f"product {product}")
            product_to_es = {}
            product_to_es["product_name_orig"] = product.get("product_name", "")
            product_to_es["category"] = category
            product_to_es["product_id_orig"] = product.get("sku_id", "")
            product_to_es["product_brand"] = product.get("brand", "")
            product_to_es["product_original_price"] = product.get("price", 0)
            product_to_es["product_sale_price"] = product.get("sale_price", 0)
            product_to_es["product_name"] = product.get("product_name", "")
            product_to_es["quantity"] = product.get("quantity", "")
            product_to_es["product_rating_value"] = product.get("product_rating_value", 0)
            product_to_es["product_rating_count"] = product.get("product_rating_count", 0)
            product_to_es["offer_code"] = product.get("offer_code", "")
            actions.append({
                "_index": index_name_product_noon,
                "_id": f"noon_test_{product_to_es['product_id_orig']}",
                "_source": {**product_to_es, "last_scraped_time": current_time, "platform": "noon", "test_run": True}
            })
        if actions:
            res = helpers.bulk(es, actions, raise_on_error=True)
            logger.info(f"write es {res}")
        logger.info(f"total products {total_products} under {category} added to ES")
        processed_categories.append(category)
    
    t2 = time.time()
    duration = int(t2-t1)
    
    logger.info(f"Scraping completed in {duration} seconds")
    logger.info(f"Total products scraped: {total_products_scraped}")
    
    # send_telegram_message(f"Noon scraping completed\nDuration: {duration} seconds\nTotal products scraped: {total_products_scraped}\nIndex: {index_name_product_noon}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Noon Scraper')
    parser.add_argument('--location', default='dubai', help='Location for scraping (default: dubai)')
    parser.add_argument('--clear', action='store_true', help='Clear existing data before scraping')
    parser.add_argument('--recreate', action='store_true', help='Delete and recreate the index')
    
    args = parser.parse_args()
    
    logger.info(f"Starting Noon scraper for location: {args.location}")
    
    if args.recreate:
        logger.info("Will recreate the index")
    elif args.clear:
        logger.info("Will clear existing data")
    
    run_noon(location=args.location, clear_existing=args.clear, recreate_index=args.recreate)