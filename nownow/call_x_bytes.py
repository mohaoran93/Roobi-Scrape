import requests
import time
import logging

logger = logging.getLogger(__name__)

API_KEY = "ggs5k8a6p9x7t"

def nownow_list_stores(latitude, longitude, page_no=1, max_retries=3):
    """Get stores with retry logic and pagination"""
    url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {
        "api_key": API_KEY,
        "latitude": latitude,
        "longitude": longitude,
        "endpoint": "nownow_uae_app_master_data",
        "endpoint_category": "nownow_uae_app_store_listing",
        "page_no": page_no
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            store_data = response.json()
            logger.info(f"NowNow XBytes get stores page {page_no}: {store_data}")
            
            # Check if we need to get more pages
            stores = store_data.get("results", {}).get("data", {}).get("stores", [])
            if len(stores) >= 200:
                logger.info(f"Page {page_no} has {len(stores)} stores, getting next page")
                try:
                    store_data_p2 = nownow_list_stores(latitude, longitude, page_no=page_no+1, max_retries=max_retries)
                    additional_stores = store_data_p2.get("results", {}).get("data", {}).get("stores", [])
                    stores.extend(additional_stores)
                    logger.info(f"Added {len(additional_stores)} stores from page {page_no+1}")
                except Exception as e:
                    logger.warning(f"Error getting page {page_no+1}: {e}")
                    
            return store_data
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for stores page {page_no}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    
    return None

def nownow_store_details(store_id, latitude, longitude, max_retries=3):
    """Get store details with retry logic"""
    url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {
        "api_key": API_KEY,
        "latitude": latitude,
        "longitude": longitude,
        "store_id": store_id,
        "endpoint": "nownow_uae_app_master_data",
        "endpoint_category": "nownow_uae_app_store_details"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for store details {store_id}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    
    return None

def nownow_produt_details(product_name, store_id, max_retries=3):
    """Get product details with retry logic"""
    url = "https://now-now.xbyteapi.com/now_now_uae_api"
    
    if product_name is None:
        raise ValueError("product_name is required")
    
    params = {
        "api_key": API_KEY,
        "store_id": store_id,
        "product_name": product_name,
        "endpoint": "nownow_uae_app_master_data",
        "endpoint_category": "nownow_uae_app_product_searching"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for product details {product_name}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    
    return None

def list_store_products(store_id, category_name, max_retries=3):
    """Get store products with retry logic"""
    params = {
        "api_key": API_KEY,
        "category_name": category_name,
        "store_id": store_id,
        "endpoint": "nownow_uae_app_master_data",
        "endpoint_category": "nownow_uae_app_product_listing"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url="https://now-now.xbyteapi.com/now_now_uae_api", params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for store products {store_id}/{category_name}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    
    return None

if __name__ == "__main__":
    import json
    
    # Enable debug logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Test coordinates
    latitude = 25.042983969226388
    longitude = 55.23489620876715
    
    print("Testing store listing...")
    stores = nownow_list_stores(latitude=latitude, longitude=longitude)
    if stores:
        print(f"Found {len(stores.get('results', {}).get('data', {}).get('stores', []))} stores")
        with open(f"temp/store_list_{latitude},{longitude}.json", "w") as f:
            json.dump(stores, f, indent=4)
    
    # Test store details
    if stores and stores.get("results", {}).get("data", {}).get("stores"):
        test_store = stores["results"]["data"]["stores"][0]
        store_id = test_store.get("Store_Code")
        
        if store_id:
            print(f"Testing store details for {store_id}...")
            store_details = nownow_store_details(store_id, latitude, longitude)
            if store_details:
                with open(f"temp/store_details_{store_id}.json", "w") as f:
                    json.dump(store_details, f, indent=4)
                
                # Test product listing
                categories = store_details.get("results", {}).get("data", {}).get("store_details", {}).get("Categories", [])
                if categories:
                    category = categories[0]
                    category_name = category.get("Category_code")
                    
                    if category_name:
                        print(f"Testing products for {store_id}/{category_name}...")
                        products = list_store_products(store_id=store_id, category_name=category_name)
                        if products:
                            with open(f"temp/store_products_{store_id}_{category_name}.json", "w") as f:
                                json.dump(products, f, indent=4)