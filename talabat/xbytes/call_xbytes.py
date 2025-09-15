import requests

TALABAT_API_KEY="ggs5k8a6p9x7t"

def get_stores(latitude, longitude):
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&latitude=25.097554095841385&longitude=55.17577022216698&endpoint_category=nownow_talabat_grocery_app_Store_listing&endpoint=nownow_uae_app_master_data
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key": TALABAT_API_KEY, "latitude": latitude, "longitude": longitude,
              "endpoint_category": "nownow_talabat_grocery_app_Store_listing",
              "endpoint": "nownow_uae_app_master_data"}
    response = requests.get(base_url, params=params).json()
    status = response.get("status")
    stores = response.get("results", {}).get("data", {}).get("Stores", {}).get("Vendors", [])
    if len(stores) == 0 or status != 200:
        print(f"talabat xbytes No stores found {latitude}, {longitude}")
        return []
    return stores

def get_store_categories(latitude, longitude, store_id):
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&latitude=25.097554095841385&longitude=55.17577022216698&store_id=33a90dda-10d3-48c1-96b9-ae37c358ffa0&endpoint_category=nownow_talabat_grocery_app_Storewise_category_listing&endpoint=nownow_uae_app_master_data
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key": TALABAT_API_KEY, "latitude": latitude, "longitude": longitude,
              "store_id": store_id,
              "endpoint_category": "nownow_talabat_grocery_app_Storewise_category_listing",
              "endpoint": "nownow_uae_app_master_data"}
    response = requests.get(base_url, params=params).json()
    status = response.get("status")
    categories = response.get("results", {}).get("data", {}).get("Category", [])
    if len(categories) == 0 or status != 200:
        print(f"talabat xbytes No categories found {latitude}, {longitude}")
        return []
    return categories

# https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&subcategory_id=5bfd46e3-4e69-4fb6-9d45-d330adb458c3&page_no=0&store_id=33a90dda-10d3-48c1-96b9-ae37c358ffa0&endpoint_category=nownow_talabat_grocery_app_categorywise_product_listing&endpoint=nownow_uae_app_master_data
def get_products(store_id, subcategory_id, page_no=0):
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key": TALABAT_API_KEY,
              "store_id": store_id, "subcategory_id": subcategory_id, "page_no": page_no,
              "endpoint_category": "nownow_talabat_grocery_app_categorywise_product_listing",
              "endpoint": "nownow_uae_app_master_data"}
    response = requests.get(base_url, params=params).json()
    status = response.get("status")
    # TODO not sure what does pagination and total_page
    pagination = response.get("results", {}).get("pagination")
    total_page = response.get("results", {}).get("total_page")
    products = response.get("results", {}).get("data", {}).get("products", [])
    if len(products) == 0 or status != 200:
        print(f"talabat xbytes No products found for store {store_id}, subcategory {subcategory_id}")
        return [], {"pagination": pagination, "total_page": total_page}
    return products, {"pagination": pagination, "total_page": total_page}

# https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&latitude=25.097554095841385&longitude=55.17577022216698&endpoint_category=nownow_talabat_mart_app_store_listing&endpoint=nownow_uae_app_master_data 
def get_mart_stores(latitude, longitude):
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key": TALABAT_API_KEY, "latitude": latitude, "longitude": longitude,
              "endpoint_category": "nownow_talabat_mart_app_store_listing",
              "endpoint": "nownow_uae_app_master_data"}
    response = requests.get(base_url, params=params).json()
    print(response)
    status = response.get("status")
    stores = response.get("results", {}).get("data", {}).get("Vendor", [])
    # There should have only few vender
    if len(stores) == 0 or status != 200:
        print(f"talabat xbytes No mart stores found {latitude}, {longitude}")
        return []
    return stores

# https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&store_id=33a90dda-10d3-48c1-96b9-ae37c358ffa0&subcategory_id=5bfd46e3-4e69-4fb6-9d45-d330adb458c3&page_no=0&endpoint_category=nownow_talabat_mart_app_product_listing&endpoint=nownow_uae_app_master_data 
def get_mart_products(store_id, subcategory_id, page_no=0):
    # TODO where is subcategory_id from
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key": TALABAT_API_KEY,
              "store_id": store_id, "subcategory_id": subcategory_id, "page_no": page_no,
              "endpoint_category": "nownow_talabat_mart_app_product_listing",
              "endpoint": "nownow_uae_app_master_data"}
    response = requests.get(base_url, params=params).json()
    status = response.get("status")
    # TODO not sure what does pagination and total_page
    pagination = response.get("results", {}).get("pagination")
    total_page = response.get("results", {}).get("total_page")
    products = response.get("results", {}).get("data", {}).get("products", [])
    if len(products) == 0 or status != 200:
        print(f"talabat xbytes No mart products found for store {store_id}, subcategory {subcategory_id}")
        return [], {"pagination": pagination, "total_page": total_page}
    return products, {"pagination": pagination, "total_page": total_page}

if __name__ == "__main__":
    get_mart_stores(25.097554095841385, 55.17577022216698)