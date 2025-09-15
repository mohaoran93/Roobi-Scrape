import requests

CARREFOUR_API_KEY="ggs5k8a6p9x7t"

def get_catgories(latitude,longitude):
    #     https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&latitude=24.4538352&longitude=54.3774014&endpoint_category=nownow_carefour_app_storewise_category_listing&endpoint=nownow_uae_app_master_data
    # old https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&location=motor%20city&endpoint_category=nownow_carefour_app_storewise_category_listing&endpoint=nownow_uae_app_master_data
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key": CARREFOUR_API_KEY, "latitude":latitude,"longitude":longitude,
              "endpoint_category": "nownow_carefour_app_storewise_category_listing",
              "endpoint": "nownow_uae_app_master_data"}
    response = requests.get(base_url, params=params)
    return response.json()

def get_products(latitude,longitude,category_id,subcategory_id):
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key= ggs5k8a6p9x7t&latitude=25.0493398&longitude=55.23125659999999&category_id=F11600000&subcategory_id=F11660100&endpoint_category=nownow_carefour_app_categorywise_product_listing&endpoint=nownow_uae_app_master_data
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key": CARREFOUR_API_KEY, "latitude": latitude, "longitude": longitude,
              "category_id": category_id,
              "subcategory_id":subcategory_id,
              "endpoint_category": "nownow_carefour_app_categorywise_product_listing",
              "endpoint": "nownow_uae_app_master_data"}
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        print(response.text)
    return response.json()

def get_products_NOW_only(latitude,longitude,category_id,subcategory_id):
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key= ggs5k8a6p9x7t&latitude=25.0493398&longitude=55.23125659999999&category_id=F11600000&subcategory_id=F11660100&endpoint_category=nownow_carefour_app_categorywise_product_listing_now&endpoint=nownow_uae_app_master_data
    base_url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key": CARREFOUR_API_KEY, "latitude": latitude, "longitude": longitude,
              "category_id": category_id,
              "subcategory_id":subcategory_id,
              "endpoint_category":"nownow_carefour_app_categorywise_product_listing_now",
              "endpoint":"nownow_uae_app_master_data"}
    response = requests.get(base_url, params=params)
    # https://now-now.xbyteapi.com/now_now_uae_api?api_key=ggs5k8a6p9x7t&latitude=25.0493398&longitude=55.23125659999999&category_id=F21630200&endpoint_category=nownow_carefour_app_categorywise_product_listing_now&endpoint=nownow_uae_app_master_data 
    return response.json()

if __name__ == "__main__":
    import json
    get_catgories = get_catgories(25.097554095841385, 55.17577022216698)
    with open("temp/carrefour_categories_api_20250324.json","w") as f:
        json.dump(get_catgories,f,indent=4)
    # res = get_products(25.097554095841385, 55.17577022216698, "F21630209")
    # with open("temp/carrefour_products_api2.json","w") as f:
    #     json.dump(res,f,indent=4)

    # res = get_products_NOW_only(25.0493398,55.23125659999999, "F21630200")
    # with open("temp/carrefour_products_api_now_product_sample.json","w") as f:
    #     json.dump(res,f,indent=4)