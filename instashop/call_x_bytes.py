import requests
from functions import get_esinstance,index_name_product
import json
es_client = get_esinstance()
# API_KEY = "7e558db946b846f8"
# API_KEY = "2pfciang9hwz9740"
API_KEY = "ggs5k8a6p9x7t" # latest key
# from tools import send_telegram_message

def instashop_list_stores_all_categories(latitude, longitude):
    # step 1: get all stores for all categories
    stores = []
    # categories = ["","Restaurants","Back to school","Pharmacies",,"Stationery & Party Supplies",
    #                   "Baby Care & Toys",,"Home & Living","Services","Bakeries & Cakes","Cosmetics & Beauty perfumes",
    #                     "Flower Shops","Butchery & Seafood",,"Organic Shops","Water","Fitness Nutrition","Electronics",
    #                     "Coffee & Tea","Giving Back"]
    categories = ["Supermarkets", "Pet Shops","Specialty & Ethnic","Fruits & Vegetables","Organic Shops"]
    store_id2category = {}
    for category in categories:
        stores_res = instashop_list_stores(latitude,longitude,category)
        store_list = stores_res.get("results",{}).get("data",{}).get("stores",[])
        print(f"Category: {category}, Stores: {len(store_list)}")
        # if len(store_list) == 0:
            # send_telegram_message(f"instashop_list_stores_all_categories: {category} has 0 stores,{latitude}, {longitude}\n response is {stores_res}")
        for store in store_list:
            store_id = store.get("store_id")
            if store_id is None:
                continue
            if store_id not in store_id2category:
                store_id2category[store_id] = category
        stores.extend(store_list)
        print(f"Total stores so far: {len(stores)}")
        print(f"store_id2category:{store_id2category}")
    return stores,store_id2category

def instashop_list_stores(latitude, longitude,category_name,page_no=1):
    # https://instashop.xbyteapi.com/instashop_uae_api?api_key=***&endpoint_category=instashop_uae_app_store_listing&latitude=25.042983969226388&longitude=55.23489620876715&category_name=Supermarkets&page_no=1
    print(f"list stores for {category_name} at page {page_no}")
    # url = "https://instashop.xbyteapi.com/instashop_uae_api" # old version
    url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key":API_KEY,"latitude":latitude,"longitude":longitude,"endpoint_category":"instashop_uae_app_store_listing",
              "category_name":category_name,"page_no":page_no}
    response = requests.get(url,params=params)
    store_data = response.json()
    print(f"store data: {store_data}")
    print(f"store size so far {len(store_data.get('results',{}).get('data',{}).get('stores',[]))}")
    if len(store_data.get("results",{}).get("data",{}).get("stores",[])) >= 200:
        store_data_p2 = instashop_list_stores(latitude,longitude,category_name,page_no=page_no+1)
        store_data.get("results",{}).get("data",{}).get("stores",[]).extend(store_data_p2.get("results",{}).get("data",{}).get("stores",[]))
        return store_data
    return response.json()


def instashop_store_details(store_id,latitude,longitude,category_name):
    # ?api_key=test_key&endpoint_category=instashop_uae_app_store_data&latitude=25.19720961280576&longitude=55.274376310408115&store_id=N9xdN5u7zq&category_name=Electronics
    # https://instashop.xbyteapi.com/instashop_uae_api?api_key=****&endpoint_category=instashop_uae_app_store_data&latitude=25.042983969226388&longitude=55.23489620876715&store_id=nsU4lWAk6i&category_name=Spinneys
    # url = "https://instashop.xbyteapi.com/instashop_uae_api" # old version
    url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key":API_KEY,"latitude":latitude,"longitude":longitude,"store_id":store_id,"category_name":category_name,"endpoint_category":"instashop_uae_app_store_data"}
    response = requests.get(url,params=params)
    print(response.json())
    store_categories = response.json().get("results",{}).get("data",{}).get("categories",None)
    return store_categories

def instashop_list_store_products_per_page(store_id,category_id,product_id_original=None,page_no=1):
    products_this_page = []
    if category_id == None and product_id_original != None:
        print(f"Product ID: {product_id_original}, try to find out category_id")
        res = es_client.search(index=index_name_product,body={"query":{"term":{"product_id.keyword":product_id_original}},"size":1})
        print(f"ES Response: {res}")
        category_id = res.get("hits",{}).get("hits",[])[0].get("_source",{}).get("category_id",None)
        if category_id == None:
            return None
    # https://instashop.xbyteapi.com/instashop_uae_api?api_key=test_key&endpoint=instashop_uae_app_master_data&endpoint_category=instashop_uae_app_product_listing&category_id=pq0y3xV0PH&store_id=N9xdN5u7zq
    # new version https://instashop.xbyteapi.com/instashop_uae_api?api_key=****&endpoint=instashop_uae_app_master_data&endpoint_category=instashop_uae_app_product_listing&category_id=rRi1XjnUt7&store_id=nsU4lWAk6i&page_no=6
    # url = "https://instashop.xbyteapi.com/instashop_uae_api" # old version
    url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key":API_KEY,"category_id":category_id,"store_id":store_id,"endpoint":"instashop_uae_app_master_data","endpoint_category":"instashop_uae_app_product_listing","page_no":page_no}
    res = requests.get(url=url,params=params)
    # print(res.json())
    with open(f"temp/instashop_list_store_products_{store_id}_{category_id}_{page_no}.json","w") as f:
        json.dump(res.json(),f,indent=4)
    print(f"scraping instashop: {store_id}_{category_id}_{page_no}")
    products = res.json().get("results",{}).get("data",{}).get("products",None)
    # remove product_price_all field using product.pop("product_price_all",None)
    if products:
        for product in products:
            product.pop("product_price_all",None)
            products_this_page.append(product)
    return products_this_page

def instashop_list_store_products(store_id,category_id,product_id_original=None):
    all_products = []
    for page_no in range(1,100):
        products_this_page = instashop_list_store_products_per_page(store_id,category_id,product_id_original,page_no)
        if products_this_page == None or len(products_this_page) == 0:
            break
        print(f"Products this page: {len(products_this_page)}")
        all_products.extend(products_this_page)
    # if len(all_products) > 0:
        # send_telegram_message(f"instashop_list_store_products: {store_id}_{category_id} has {len(all_products)} products")
    return all_products

def validation_by_product_difference(lat1,lng1,lat2,lng2):
    # get all stores for both lat1,lng1 and lat2,lng2 and compare the store_id difference
    stores1,_ = instashop_list_stores_all_categories(lat1,lng1)
    stores2,_ = instashop_list_stores_all_categories(lat2,lng2)
    store_ids1 = [store.get("store_id") for store in stores1]
    store_ids2 = [store.get("store_id") for store in stores2]
    store_ids_diff = list(set(store_ids1) - set(store_ids2))
    print(f"Stores difference: {len(store_ids_diff)}, total stores ({lat1},{lng1}): {len(store_ids1)} ,total stores({lat2},{lng2}): {len(store_ids2)}")
    return store_ids_diff

if __name__ == "__main__":
    import json
    # test 1
    # 25.372396787184577, 55.39592658799068
    latitude1 = 25.086611000097484
    longitude1 = 55.14784192040921
    latitude2 = 25.0862491641412
    longitude2 = 55.148190389421465

    # store_ids_diff = validation_by_product_difference(latitude1,longitude1,latitude2,longitude2)
    # exit()

    # stores = instashop_list_stores_all_categories(latitude=latitude, longitude=longitude)
    # print(stores)
    # with open(f"temp/store_list_test1_{latitude},{longitude}.json","w") as f:
    #     json.dump(stores,f,indent=4)
    # exit()

    # # test 2
    # instashop_store_details("l75Dky1Dvl",25.0235524,55.1485911)
    # # test 3
    # store_id = "nsU4lWAk6i"
    latitude,longitude=25.042983969226388,55.23489620876715
    latitude,longitude=25.042983969226388,55.23489620876715
    # all_cateogries = instashop_store_details(store_id=store_id,latitude=latitude, longitude=longitude)
    # for category in all_cateogries:
    #     print(f"About to list products by {store_id},and  {category} ")
    #     category_id = category["category_id"]
    #     res = instashop_list_store_products(store_id=store_id,category_id=category_id)

    # test 4
    categories = ["Supermarkets", "Pet Shops","Specialty & Ethnic","Fruits & Vegetables","Organic Shops"]
    for category in categories:
        res = instashop_list_stores(latitude=latitude,longitude=longitude,category_name=category)
        with open(f"temp/store_list_test4_{latitude},{longitude}_{category}.json","w") as f:
            json.dump(res,f,indent=4)