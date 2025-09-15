import requests
import json
import time
import logging

logger = logging.getLogger(__name__)

CAREEM_API_KEY = "ggs5k8a6p9x7t"

def listing_categories(latitude, longitude, max_retries=3):
    """Get categories with retry logic"""
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {
        "api_key": CAREEM_API_KEY, 
        "latitude": latitude, 
        "longitude": longitude,
        "endpoint_category": "nownow_careem_app_storewise_category_listing",
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
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise

def listing_products(latitude, longitude, zone_id, subcategory_id, page_no=1, max_retries=3):
    """Get products with retry logic"""
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {
        "api_key": CAREEM_API_KEY, 
        "latitude": latitude, 
        "longitude": longitude, 
        "zone_id": zone_id,
        "subcategory_id": subcategory_id, 
        "page_no": page_no,
        "endpoint_category": "nownow_careem_app_categorywise_product_listing",
        "endpoint": "nownow_uae_app_master_data"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for products page {page_no}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise

def list_all_products(latitude, longitude, zone_id, subcategory_id):
    """Get all products with improved pagination logic"""
    page_no = 1
    all_products = []
    
    while True:
        try:
            logger.info(f"Fetching page {page_no} for subcategory {subcategory_id}")
            response = listing_products(latitude, longitude, zone_id, subcategory_id, page_no)
            
            # Check if response is valid
            if not response or "results" not in response:
                logger.error(f"Invalid response structure for page {page_no}")
                break
                
            results = response.get("results", {})
            
            # Get products from current page FIRST
            products = results.get("data", {}).get("products", [])
            if products:
                all_products.extend(products)
                logger.info(f"Added {len(products)} products from page {page_no}")
            else:
                logger.info(f"No products found on page {page_no}")
                
            # Check pagination info AFTER getting products
            pagination = results.get("pagination")
            
            if not pagination:
                logger.warning(f"No pagination info found for page {page_no}, assuming single page")
                # If no pagination info and we got products, assume this is the only page
                # If no pagination info and no products, we're done
                break
                
            # Check if we have more pages
            current_page = pagination.get("current_page", page_no)
            total_pages = pagination.get("total_pages", 1)
            
            logger.info(f"Pagination: current_page={current_page}, total_pages={total_pages}")
            
            if current_page >= total_pages:
                logger.info(f"Reached last page {current_page}/{total_pages}")
                break
                
            # If no products on this page, don't continue
            if not products:
                logger.info(f"No more products found, stopping pagination")
                break
                
            page_no += 1
            time.sleep(0.5)  # Small delay between requests
            
        except Exception as e:
            logger.error(f"Error fetching page {page_no}: {e}")
            break
    
    logger.info(f"Total products collected: {len(all_products)}")
    return all_products

if __name__ == "__main__":
    # Enable more detailed logging for testing
    logging.basicConfig(level=logging.DEBUG)
    
    zone_id = 1035868
    subcategory_id = 1036029368
    
    # Test single API call first
    print("Testing single API call...")
    response = listing_products(25.042983969226388, 55.23489620876715, zone_id, subcategory_id, 1)
    print(f"API Response keys: {list(response.keys()) if response else 'None'}")
    
    if response and "results" in response:
        results = response["results"]
        print(f"Results keys: {list(results.keys())}")
        
        if "data" in results:
            data = results["data"]
            print(f"Data keys: {list(data.keys())}")
            
            if "products" in data:
                products = data["products"]
                print(f"Found {len(products)} products")
                if products:
                    print(f"First product keys: {list(products[0].keys())}")
            else:
                print("No 'products' key in data")
        else:
            print("No 'data' key in results")
            
        if "pagination" in results:
            pagination = results["pagination"]
            print(f"Pagination: {pagination}")
        else:
            print("No 'pagination' key in results")
    
    # Test full scraping
    print("\nTesting full scraping...")
    res = list_all_products(25.042983969226388, 55.23489620876715, zone_id, subcategory_id)
    print(f"Total products: {len(res)}")
    
    if res:
        with open(f'temp/products_data_all_products_{zone_id}_{subcategory_id}.json', 'w') as file:
            json.dump(res, file, indent=4)
        print("Sample product saved to temp/ directory")