import requests
import logging
logger = logging.getLogger("Noon-Scrapy-xbytes")
NOON_API_KEY="ggs5k8a6p9x7t"

def get_categories(location="dubai"):
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key=test_key&location=dubai&endpoint_category=nownow_noon_app_storewise_category_listing&endpoint=nownow_uae_app_master_data
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {
        "api_key": NOON_API_KEY,
        "location": location,
        "endpoint_category": "nownow_noon_app_storewise_category_listing",
        "endpoint": "nownow_uae_app_master_data"
    }
    response = requests.get(base_url, params=params)
    return response.json()

def get_products(location,store_name,page_no=1):
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key=test_key&location=dubai&store_name=grocery-store&page_no=1&endpoint_category=nownow_nooon_app_categorywise_product_listing&endpoint=nownow_uae_app_master_data
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {
        "api_key": NOON_API_KEY,
        "location": location,
        "store_name": store_name,
        "page_no": page_no,
        "endpoint_category": "nownow_nooon_app_categorywise_product_listing",
        "endpoint": "nownow_uae_app_master_data",
    }
    response = requests.get(base_url, params=params)
    logger.debug(f"Noon get_products response: {response.json()}")
    return response.json()

def get_all_products_with_pagenation(location,store_name):
    products = []
    page_no = 1
    while True:
        response = get_products(location,store_name, page_no)
        result = response.get("results",{})
        
        # Handle different pagination response formats
        if "pagination" in result:
            pagination_info = result.get("pagination", "1")
            if isinstance(pagination_info, str):
                current_page = int(pagination_info)
                total_pages = current_page  # Assume single page if only string format
            else:
                current_page = int(result.get("current_page", 1))
                total_pages = int(result.get("total_pages", 1))
        else:
            current_page = page_no
            total_pages = 1
            
        page_products = result.get("data",{}).get("products",[])
        if not page_products:
            break
            
        products.extend(page_products)
        logger.info(f"current_page: {current_page}, total_pages: {total_pages}, total_products: {len(products)}")
        
        # Check if we should continue pagination
        if len(page_products) == 0 or current_page >= total_pages:
            break
            
        page_no += 1
        
    return products

if __name__ == "__main__":
    import json
    # categories = get_categories(location="dubai")
    # with open("temp/noon_categories_sample.json","w") as f:
    #     json.dump(categories,f,indent=4)
    # store_name = "pet-supplies"
    # store_name = "/grocery-store/home-care-and-cleaning"
    store_name = "grocery-store"
    store_name_str  = store_name.replace("/","_").replace("-","_")
    products = get_products(location="dubai",store_name=store_name)

    with open(f"temp/noon_products_sample{store_name_str}.json","w") as f:
        json.dump(products,f,indent=4)