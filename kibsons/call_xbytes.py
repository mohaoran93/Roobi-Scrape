import requests
import json
import logging
import traceback
import time

KBS_API_KEY = "ggs5k8a6p9x7t"
base_url = "https://now-now.xbyteapi.com/now_now_uae_api"

logger = logging.getLogger(__name__)

def get_categories(max_retries=3):
    # store_name=sainbury,supervalu,"m and s"
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&store_name=sainbury&endpoint_category=nownow_kibson_app_category_listing
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&endpoint_category=nownow_kibson_app_category_listing&endpoint=nownow_uae_app_master_data
    """Get categories with retry logic"""
    params = {
        "api_key": KBS_API_KEY,
        "endpoint_category": "nownow_kibson_app_category_listing",
        "endpoint": "nownow_uae_app_master_data"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for categories: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise

def get_products(category_name, page_no=1, max_retries=3):
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&category_name=Pantry&page_no=3&endpoint_category=nownow_kibson_app_categorywise_product_listing&endpoint=nownow_uae_app_master_data
    """Get products with retry logic"""
    params = {
        "api_key": KBS_API_KEY,
        "category_name": category_name,
        "page_no": page_no,
        "endpoint_category": "nownow_kibson_app_categorywise_product_listing",
        "endpoint": "nownow_uae_app_master_data"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for products {category_name} page {page_no}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise

def get_all_products(category_name):
    """Get all products for a category with pagination"""
    page_no = 1
    all_products = {
        "Products": {
            category_name: []
        }
    }
    
    while True:
        logger.info(f"Fetching {category_name} page {page_no}")
        try:
            get_products_res = get_products(category_name, page_no)
            
            if not get_products_res:
                logger.warning(f"No response for {category_name} page {page_no}")
                break
                
            products = get_products_res.get("Products", {}).get(category_name, [])
            
            if not products:
                logger.info(f"No more products for {category_name} at page {page_no}")
                break
            
            all_products["Products"][category_name].extend(products)
            logger.info(f"Total products for {category_name}: {len(all_products['Products'][category_name])}")
            
            page_no += 1
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Error fetching products for {category_name} at page {page_no}: {e}")
            break
    
    return all_products.get("Products")

def get_products_by_search(product_name, page_no=1):
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&search_keyword=pantry&page_no=2&endpoint_category=nownow_kibson_app_search_keyword&endpoint=nownow_uae_app_master_data
    """Search products by keyword"""
    params = {
        "api_key": KBS_API_KEY,
        "search_keyword": product_name,
        "page_no": page_no,
        "endpoint_category": "nownow_kibson_app_search_keyword",
        "endpoint": "nownow_uae_app_master_data"
    }
    response = requests.get(base_url, params=params)
    return response.json()

if __name__ == "__main__":
    # Test the API functions
    logging.basicConfig(level=logging.DEBUG)
    
    print("Testing categories...")
    categories = get_categories()
    print(f"Categories: {categories}")
    
    if categories and categories.get("Categories"):
        category = categories["Categories"][0]
        print(f"\nTesting products for category: {category}")
        products = get_all_products(category)
        print(f"Products found: {len(products.get(category, [])) if products else 0}")