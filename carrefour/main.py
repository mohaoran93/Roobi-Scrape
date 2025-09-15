import time
import os
from elasticsearch import NotFoundError,helpers
import logging
import h3
from datetime import datetime
from carrefour.call_xbytes import get_catgories,get_products
from hashlib import md5
import json
import argparse

# from tools import send_telegram_message
from functions import (
    get_esinstance,
    carrefour_index_name_product,
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
logger = logging.getLogger("Carrefour-Scrapy")

es = get_esinstance()
total_products = 0
current_time_iso = datetime.utcnow()
current_time = current_time_iso.isoformat()

excluded_categories_ids = ["NF4000000","NF1200000","NF5000000","NF1400000","NF2300000","NF9000000"]
excluded_categories_name = ["Electronics & Appliances","Smartphones, Tablets & Wearables",
                            "Fashion, Accessories & Luggage","Toys & Outdoor","Automotive","Kiosk"]


def go_through_all_categories(latitude,longitude,categories,total_products=0,h3_cell=None):
    for category in categories:
        if category.get("id") in excluded_categories_ids or category.get("name") in excluded_categories_name:
            logger.info(f"excluded category {category.get('name')} | Level {category.get('level')}")
            continue
        category_id = category.get("id")
        logger.info(f"category id {category_id}, category name {category.get('name')} | subcategory size {len(category.get('subcategories', []))} | Level {category.get('level')}")
        for sub_category in category.get("subcategories", []):
            subcategory_id = sub_category.get("id")
            logger.info(f"has subcategories {subcategory_id} | subcategory size {len(category.get('subcategories', []))} | Level {category.get('level')}")
            products_response = get_products(latitude=latitude,longitude=longitude,category_id=category_id,subcategory_id=subcategory_id)
            size = len(products_response.get('result',[]))
            logger.info(f"products response size {size}")
            total_products += size
            products = products_response.get("result",None)
            if products == None or len(products) == 0:
                # send_telegram_message(f"warning carrefour TEST scraping took 0 product\ncategory info: {category}\nrequests response msg: {products_response.get('msg')}")
                logger.warning(f"products not found for {products_response}")
                continue  # changed from return to continue
            write_products_to_es(products,h3_cell,subcategory_id)

def write_products_to_es(products,h3_cell,deepest_category_id):
        actions = []
        for product in products:
            product_info_to_es = {}
            product_info_to_es["product_brand"] = product.get("brand_name")
            product_price = product.get('discounted_price')
            if product_price == None or product_price == 0:
                product_price = float(product.get('price', product.get('minBuyingValue', 0)))
            else:
                product_price = float(product_price)
            product_info_to_es["product_sale_price"] =  product_price
            product_info_to_es["product_original_price"] = float(product.get('price', 0))
            product_info_to_es["product_image"] = product.get('img_url')
            product_info_to_es["product_id_orig"] = product.get('product_id')
            product_info_to_es["category"] = product.get('category_level1_name')
            try:
                product_info_to_es["sellerName"] = json.loads(product.get('offers', '[]'))[0].get("sellerName")
            except (json.JSONDecodeError, IndexError, KeyError):
                product_info_to_es["sellerName"] = "Carrefour"
            # TODO unify those names
            product_info_to_es["is_express"] = product.get('is_express')
            product_info_to_es["min_order"] = product.get('min_order')# item_size
            product_info_to_es["unit_measure"] = product.get('unit_measure')
            product_info_to_es["product_name"] = product.get('product_name')
            product_info_to_es["is_express"] = product.get('is_express')
            product_info_to_es["item_size"] = product.get('item_size')
            action = {
                "_index": carrefour_index_name_product,
                "_id": md5((str(product_info_to_es["product_id_orig"])+"cf_test").encode()).hexdigest(),
                "_source": {**product_info_to_es,"last_scraped_time":current_time,"h3_cell":h3_cell,"deepest_category_id":deepest_category_id,"test_run":True,"platform":"carrefour"}
            }
            logger.debug("product original info to es {}".format(product))
            logger.debug("product info to es {}".format(action["_id"]))
            logger.debug("product name {}".format(product.get('product_name')))
            actions.append(action)
        helpers.bulk(es, actions,raise_on_error=True)

def run_carrefour(latitude, longitude, resolution=10, clear_existing=False, recreate_index=False):
    global total_products
    t1 = time.time()
    
    # Handle index management
    if recreate_index:
        logger.info("Recreating index...")
        delete_index(es, carrefour_index_name_product)
        create_index_if_not_exists(es, carrefour_index_name_product)
    elif clear_existing:
        logger.info("Clearing existing data...")
        clear_index(es, carrefour_index_name_product)
    else:
        create_index_if_not_exists(es, carrefour_index_name_product)
    
    h3_cell = h3.geo_to_h3(latitude, longitude, resolution)
    geo = {"lat": latitude, "lon": longitude}
    logger.info("received geo location {}".format(geo))
    logger.info(f"Using index: {carrefour_index_name_product}")
    
    # step 1: get all categories
    LOAD_FROM_FILE = False
    os.makedirs("temp", exist_ok=True)
    if LOAD_FROM_FILE:
        with open(f'temp/categories_data_{h3_cell}.json', 'r') as file:
            categories_api_res = json.load(file)
    else:
        categories_api_res = get_catgories(latitude, longitude)
        with open(f'temp/categories_data_{h3_cell}.json', 'w') as file:
            json.dump(categories_api_res, file, indent=4)
    # logger.info(f"categories response {categories_api_res}")
    categories = categories_api_res.get("locations",[{}])[0].get("categories",[])
    go_through_all_categories(latitude,longitude,categories,total_products,h3_cell)
    t2 = time.time()
    duration = int(t2-t1)
    
    logger.info(f"Scraping completed in {duration} seconds")
    logger.info(f"Total products scraped: {total_products}")
    # send_telegram_message(f"Carrefour scraping took {duration} seconds to finish\n total products scraped {total_products}\nIndex: {carrefour_index_name_product}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Carrefour Scraper')
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
    
    run_carrefour(latitude, longitude, clear_existing=args.clear, recreate_index=args.recreate)