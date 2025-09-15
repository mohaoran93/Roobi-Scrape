import redis
import os
import threading
import time
from datetime import datetime
import json
from app.api import Api
from elasticsearch import Elasticsearch,helpers
from dotenv import load_dotenv
import requests
import traceback
from demo import spider,compare
import logging
import sys
from hashlib import md5
sys.path.append(".")

from kibsons.call_xbytes import get_products as kibsons_get_products
# from carrefour.scrap_realtime_price.carrefour_product import carrefour_stock
from carrefour.call_xbytes import get_products as carrefour_get_products
from nownow.call_x_bytes import nownow_list_stores, nownow_store_details, nownow_produt_details
from instashop.call_x_bytes import instashop_list_store_products
from careem_quick.call_xbytes import list_all_products
from store_status import STORE_CLOSED,STORE_OPEN,STORE_NOTFOUND,StoreStatus

load_dotenv("realtime_worker/.env")

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
HARDCODE_LATLNG=os.getenv("HARDCODE_LATLNG",None)
if HARDCODE_LATLNG:
    hardcode_lat = float(HARDCODE_LATLNG.split(",")[0].strip())
    hardcode_lng = float(HARDCODE_LATLNG.split(",")[1].strip())
else:
    hardcode_lat = None
    hardcode_lng = None

session = requests.Session()

r = redis.Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)

es_host = os.getenv("ES_HOST")
DB_UPDATE_API = os.getenv("DB_UPDATE_API")
ELASTIC_PASSWORD=os.getenv("ELASTIC_PASSWORD")
es_client = Elasticsearch(es_host,
    basic_auth=("elastic", ELASTIC_PASSWORD)
)
scrapy_product_info_careem_quick = os.getenv("scrapy_product_info_careem_quick")
carrefour_index_name_product = os.getenv("carrefour_index_name_product")
store_status_processor = StoreStatus()

def id_function(info):
    # same as matching_new_location.py
    return md5((str(info["product_id_orig"])+str(info["platform"])+str(info["store_id"])+str(info["product_name"])).encode()).hexdigest()

def do_search_product(platform, store_id, product_id, product_name, superCategoryCode, lat, lng, shop_type, bid, vendor_id, delivery_time,category,index,*args):
    api = Api()
    if platform == 'talabat':
        if not (shop_type and bid):
            # vendor_id is none for shop_type 2
            return {
                'code': 500,
                'message': 'Missing parameters',
                'data': {'platform': platform, 'product_id': product_id,'vendor_id':vendor_id,"shop_type":shop_type,"bid":bid}
            }
        store_status = store_status_processor.get_store_status(store_id,"talabat",lat,lng)
        if store_status == STORE_CLOSED or store_status == STORE_NOTFOUND:
            return {"code":200,"message":STORE_CLOSED,'data':{"product_active":False}}
        return api.search_one(vendor_id=vendor_id, product_id=product_id, shop_type=shop_type, bid=bid)

    elif platform == 'instashop':
        if not store_id:
            return {
                'code': 500,
                'message': 'Missing parameters',
                'data': {'platform': platform, 'product_id': product_id}
            }
        
        # store_status = store_status_processor.get_store_status(store_id,"instashop",lat,lng)
        store_status = STORE_OPEN # skip store status check
        if store_status == STORE_CLOSED or store_status == STORE_NOTFOUND:
            return {"code":200,"message":STORE_CLOSED,'data':{"product_active":False}}
        products = instashop_list_store_products(store_id=store_id,category_id=None,product_id_original=product_id)
        """ {
        "store_id": "l75Dky1Dvl",
        "location_id": "",
        "product_id": "QPPOkDTyid",
        "product_name": "Colgate Double Action Green & White Medium Toothbrush",
        "product_category": "Oral Care",
        "product_subcategory": "",
        "product_brand": "Colgate",
        "product_price": 22,
        "product_discount_price": 3.95,
        "product_availability": "In Stock",
        "product_link": "",
        "product_image": "https://cdn.instashop.ae/w-259c96b5-72f5-4d2b-85c7-ab77c1f8a76d39b281b9f318cedd538b653ce359062b_IMG_0041.JPG",
        "category_id": "dmEX4Wa7XD",
        "first_category_id": "dmEX4Wa7XD",
        "second_category_id": "",
        "packagingString": "1 pcs",
        "pricePerUnit": "",
        "excludedFromMinimumOrder": false
        } """
        if len(products) == 0:
            return {'code': 200, 'message': 'success', 'data':{"product_active":False} }
        final_res = {"product_active":False}
        actions_to_es = []
        now_time = datetime.utcnow().isoformat()
        print(f"product_id working on {product_id}")
        for product in products:
            print(f"get product {product}")
            product_active = product.get("product_availability") == "In Stock"
            product_price = product.get("product_discount_price")
            if product_price == None or product_price == 0:
                product_price = product.get("product_price")
            if product_id == product.get("product_id"):
                final_res = {
                        'platform': 'instashop',
                        'product_id': product_id,
                        'product_name': product["product_name"],
                        'product_price': product_price,
                        'product_active': product_active,
                        'store_id': store_id
                    }
                print(f"final_res {final_res}")
            product_to_es = {"product_sale_price":product_price,
                             "is_in_stock":product_active,"is_in_stock_updated_at":now_time,"updated_at":now_time}
            _id = id_function({"product_id_orig":product.get("product_id"),"platform":"is","store_id":store_id,"product_name":product.get("product_name")})
            print(f"product_to_es {_id}: {product_to_es}")
            actions_to_es.append({"_op_type": "update","_index":index,"_id":_id,"doc":product_to_es})

        try:
            logging.debug(f"bulk update kibsons {actions_to_es[:10]}")
            helpers.bulk(es_client, actions_to_es)
        except Exception as e:
            traceback.print_exc()
            logging.error(f"bulk update kibsons error {e}")

        return {'code': 200, 'message': 'success', 'data': final_res}

    elif platform == 'nownow':
        # disable using nownow service for testing perpose
        # return {"code":500,"message":"nownow not implemented yet"}
        # '{"_index": "products_geo_info_8a43a130114ffff_2024052805", "_id": "7cf9ca605ded15d050f597910decc3de", "_score": null, "_source": {"product_id_orig": "P14236549177A", "product_name": "Super Q Special Palabok Cornstarch Stick Noodles", "product_sale_price": 3.75, "platform": "nn", "store_id": "RRCHSPD6M6", "store_name": "R Rich Supermarket", "vendor_id": null, "bid": null, "shop_type": null, "product_id": "fb72076164fa3d17b977d742cad17d71", "popularity": 2}, "sort": [3.75], "latitude": "25.0867931", "longitude": "55.1475512"}'
        # use x bytes service
        if not (product_name and store_id):
            return {
                'code': 500,
                'message': 'Missing parameters',
                'data': {'platform': platform, 'product_id': product_id,'product_name':product_name,'store_id':store_id}
            }
        
        # skip store status
        # store_status = store_status_processor.get_store_status(store_id,"nownow",lat,lng)
        # if store_status == STORE_CLOSED or store_status == STORE_NOTFOUND:
        #     return {"code":200,"message":STORE_CLOSED,'data':{"product_active":False}}

        xbytes_res = nownow_produt_details(product_name,store_id)
        logging.debug(f"nownow real time xbytes response {xbytes_res}")
        if xbytes_res.get("results").get("status") != 200:
            logging.error(f"nownow real time xbytes issue {xbytes_res}")
            return xbytes_res
        products = xbytes_res.get("results").get("data").get("products")
        if products == None:
            logging.error(f"nownow real time xbytes issue products are none {xbytes_res}")
            return xbytes_res
        for product in products:
            # find exact product_name and 
            if product.get("Product_Code") == product_id:
                price = product.get("Discounted_Price")
                product_active = True
                data = {
                    'platform': 'nownow',
                    'product_id': product_id,
                    'product_name': product_name,
                    'product_price': price, # reqired field
                    'product_active': product_active,# reqired field
                    'store_id': store_id
                }
                return {'code': 200, 'message': 'success', 'data':data }
        return {'code': 200, 'message': 'success', 'data':{"product_active":False} }

    elif platform == 'carrefour':            
        try:
            res = es_client.search(index=carrefour_index_name_product,body={"query":{"term":{"product_id_orig":product_id}}})
            category_id = res.get("hits").get("hits")[0].get("_source",{}).get("deepest_category_id")
        except Exception as e:
            traceback.print_exc()
            logging.error(f"carrefour get category_id error {e}")
            return {'code': 500,'message': str(e),'data': {'platform': platform, 'product_id': product_id}}
        if category_id == None:
            return {'code': 500,'message': "product_id did not get deepest_category_id",'data': {'platform': platform, 'product_id': product_id}}
        
        products_res = carrefour_get_products(lat,lng,category_id)
        for product in products_res.get("result",[]):
            if product.get("product_id") == product_id:
                product_price = product.get("discounted_price")
                if product_price == None or product_price == 0:
                    product_price = product.get("price")
                return {'code': 200, 'message': 'success', 'data':{"product_active":True,"product_price":product_price}}
        return {'code': 500,'message': "product not found",'data': {'platform': platform, 'product_id': product_id}}
            
    elif platform == "kibsons":
        xbytes_res = kibsons_get_products(category)
        products = xbytes_res.get("Products",{}).get(category,[])
        # test
        # products = [{'Title': 'Chicken Nuggets', 'Actual Price': 27.7, 'Discount Price': 0, 'Price Per Size': 'pack', 'Description': '500g', 'Brand': 'Skinny Genie', 'Featured': ['Local', 'Gluten-free'], 'Popularity Score': 2306, 'Origin Country': 'UAE', 'IsFavorite': False, 'Product ID': 160932, 'Product Code': 'CHIBRAESG316L1', 'InStock': 'AVAILABLE', 'ImageURL': 'https://kibsecomimgstore.blob.core.windows.net/products/display/HPL_CHIBRAESG316L1_20210801140118.jpg'}, {'Title': 'Pumpkin Cubes', 'Actual Price': 12.5, 'Discount Price': 0, 'Price Per Size': 'pack', 'Description': '450g', 'Brand': '2 Be Bio', 'Featured': [], 'Popularity Score': 1504, 'Origin Country': 'Poland', 'IsFavorite': False, 'Product ID': 152200, 'Product Code': 'PUMHOPL2B450SO', 'InStock': 'AVAILABLE', 'ImageURL': 'https://kibsecomimgstore.blob.core.windows.net/products/display/pumhopl2b450so_730366.jpg'}, {'Title': 'Bananas', 'Actual Price': 20, 'Discount Price': 0, 'Price Per Size': 'pack', 'Description': 'Ripe & Chopped - 300g', 'Brand': 'Pack’D', 'Featured': ['Vegan'], 'Popularity Score': 40373, 'Origin Country': 'UK', 'IsFavorite': False, 'Product ID': 190182, 'Product Code': 'BANCHUKPD300SF', 'InStock': 'AVAILABLE', 'ImageURL': 'https://kibsecomimgstore.blob.core.windows.net/products/display/HPL_banchukpd300sf_20230731164627.jpg'}, {'Title': 'Hass Avocado', 'Actual Price': 40.25, 'Discount Price': 0, 'Price Per Size': 'pack', 'Description': '500g', 'Brand': 'M&S', 'Featured': [], 'Popularity Score': 20259, 'Origin Country': 'UK', 'IsFavorite': False, 'Product ID': 185736, 'Product Code': 'FLOGFUKMS1KGSF', 'InStock': 'AVAILABLE', 'ImageURL': 'https://kibsecomimgstore.blob.core.windows.net/products/display/HPL_FLOGFUKMS1KGSF_20230614160755.jpg'}, {'Title': 'Baby Spinach Leaf', 'Actual Price': 16.25, 'Discount Price': 0, 'Price Per Size': 'pack', 'Description': 'Whole - 900g', 'Brand': "Sainsbury's", 'Featured': [], 'Popularity Score': 18331, 'Origin Country': 'Spain', 'IsFavorite': False, 'Product ID': 153596, 'Product Code': 'SPIWHESSB1KGSF', 'InStock': 'AVAILABLE', 'ImageURL': 'https://kibsecomimgstore.blob.core.windows.net/products/display/HPL_SPIWHESSB1KGSF_20220830103846.jpg'}]
        logging.debug(f"kibsons real time xbytes products sample {products[:5]}")
        
        if len(products) == 0:
            return {'code': 200, 'message': 'success', 'data':{"product_active":False} }
        final_res = {}
        actions_to_es = []
        for product in products:
            if product_id == product.get("Product ID"):
                if product.get("InStock") == "AVAILABLE":
                    final_res = {
                        'platform': 'kibsons',
                        'product_id': product_id,
                        'product_name': product_name,
                        'product_price': product.get("Actual Price"),
                        'product_active': True,
                        'store_id': store_id
                    }
                else:
                    # "OUT OF STOCK"
                    final_res = {
                        'platform': 'kibsons',
                        'product_id': product_id,
                        'product_name': product_name,
                        'product_price': product.get("Actual Price"),
                        'product_active': False,
                        'store_id': store_id
                    }

                # "_index": kibsons_index_name_product,
                # "_id": "{}_{}".format(product_to_es["product_id"],product_to_es["product_code"]),
                # "_source": {**product_to_es,"last_scraped_time":current_time}
                # TODO
            now_time = datetime.utcnow().isoformat()
            product_to_es = {"product_sale_price":product.get("Actual Price"),
                             "is_in_stock":product.get("InStock") == "AVAILABLE","is_in_stock_updated_at":now_time,"updated_at":now_time}
            actions_to_es.append({"_op_type": "update","_index":index,"_id":
                                  id_function({"product_id_orig":product.get("Product ID"),"platform":"kbs","store_id":store_id,"product_name":product.get("Title")}),
                                  "doc":product_to_es})
        try:
            logging.debug(f"bulk update kibsons {actions_to_es[:10]}")
            helpers.bulk(es_client, actions_to_es)
        except Exception as e:
            traceback.print_exc()
            logging.error(f"bulk update kibsons error {e}")
        if final_res.get("product_active") == True:
            return {'code': 200, 'message': 'success', 'data':final_res }
        else:
            return {"code":200,"message":STORE_CLOSED,'data':{"product_active":False}}

    elif platform == "careem_quick":
        """
        ('realtime_stock_cq_products', '{"_index": "products_geo_info_8a43a1220467fff_20241114", "_id": "2f4b690611ca18cfedcca01254081940", "_score": null, "_source": {"product_sale_price": 14.0, "product_id_orig": 2606395450, "product_name": "Tomato Beef Holland 1 kg", "platform": "cq", "store_id": "cq", "store_name": "careem_quick", "category": null, "vendor_id": null, "bid": null, "shop_type": null, "product_id": "672527d134ff52e82753bc9e4180e072", "popularity": 2}, "sort": [14.0], "latitude": "25.087054289271904", "longitude": "55.14804929494858"}')
        """
        res = es_client.search(index=scrapy_product_info_careem_quick,body={"query":{"term":{"product_id":product_id}}})
        source = res.get("hits").get("hits")[0].get("_source",{})
        zone_id = source.get("zone_id")
        subcategory_id = source.get("subcategory_id")
        last_scraped_time = source.get("last_scraped_time")
        current_time_iso = datetime.utcnow()
        current_time = current_time_iso.isoformat()
        if last_scraped_time and (current_time_iso - datetime.fromisoformat(last_scraped_time)).seconds < 60*60*24:
            # less than 24 hours
            product_price = source.get("discounted_price",product.get("original_price"))
            if product_price != None:
                return {'code': 200, 'message': 'success', 'data':{"product_active":True,"product_price":product_price}}
        if zone_id == None or subcategory_id == None:
            return {"code":200,"message":"success","data":{"product_active":False}}
        products = list_all_products(lat, lng, zone_id, subcategory_id)
        if len(products) == 0:
            return {"code":200,"message":"success","data":{"product_active":False}}
        final_res = {}
        for product in products:
            if product_id == product.get("product_id"):
                product_price = product.get("discounted_price",product.get("original_price"))
                final_res = {
                    'platform': 'careem_quick',
                    'product_id': product_id,
                    'product_name': product_name,
                    'product_price': product_price,
                    'product_active': True,
                    'store_id': store_id
                }

        actions = [
                {
                    "_index": scrapy_product_info_careem_quick,
                    "_id": "{}_{}".format("careem_quick",product["product_id"]),
                    "_source": {**product,"last_scraped_time":current_time,"subcategory_id":subcategory_id,"zone_id":zone_id}
                }
                for product in products
            ]
        helpers.bulk(es_client, actions,raise_on_error=True)
        
        if final_res.get("product_active") == True and final_res.get("product_price") != None:
            return {'code': 200, 'message': 'success', 'data':final_res }
        else:
            return {"code":200,"message":"success",'data':{"product_active":False}}


def consume_messages(queue_name):
    # r = redis.Redis(host='localhost', port=6379, db=0)
    platform_name_conv = {"cf_std":"carrefour","cf_now":"carrefour","nn":"nownow","tb":"talabat","is":"instashop","kbs":"kibsons","cq":"careem_quick"}
    while True:
        try:
            # 从队列中获取消息，timeout=10 表示最多等待 10 秒
            message = r.blpop(queue_name,timeout=10) # 1. rpush 2. hope blpop won't time out like lpop
            logging.debug(f"message type {type(message)}")
            if message:
                logging.debug(f"message from queue: {message}")
                hit = json.loads(message[1])
                source = hit["_source"]
                platform_code = source["platform"]

                # should put it in exception final
                # 获取任务的索引和产品 ID
                index = hit["_index"]
                product_id_for_matching = source["product_id"]
                try:
                    if source.get("shop_type") != None:
                        shop_type = int(source["shop_type"])
                    else:
                        shop_type = source.get("shop_type")
                    
                    one_data = {"platform":platform_name_conv[platform_code],
                                "store_id":source["store_id"],
                                "product_id":source["product_id_orig"],
                                "product_name":source["product_name"],
                                "superCategoryCode":source.get("superCategoryCode"),
                                # "store_name":source["store_name"],
                                "category":source.get("category"),
                                "vendor_id":source.get("vendor_id"),
                                "shop_type":shop_type,
                                "bid":source.get("bid"),
                                "lat": hardcode_lat if hardcode_lat else hit["latitude"],
                                "lng": hardcode_lng if hardcode_lat else hit["longitude"],
                                "delivery_time":"Now", # Now or STANDARD
                                "index":index
                                }
                    # 输出日志信息，显示 Elasticsearch 索引和文档 ID
                    logging.debug("es_index {} _id {}".format(hit["_index"],hit["_id"]))
                    # 调用实时价格检查函数，并获取最新的商品信息
                    latest_info = do_search_product(**one_data)
                    if latest_info.get("code") != 200 or latest_info.get("message") != "success":
                        logging.error(f"latest_info: {latest_info}")
                        # log error

                    # 获取最新的商品价格和激活状态

                    latest_price =  latest_info.get("data",{}).get('product_price')
                    is_active =  latest_info.get("data",{}).get('product_active',False)
                    # need to update current_time even the price is the same
                    # 获取当前时间
                    current_time = datetime.utcnow().isoformat()
                    # 如果最新价格和激活状态存在，则更新 Elasticsearch 中的数据
                    if latest_price and is_active:
                        # print("product name {} old price {} latest price {}".format(latest_info.get("data").get('product_name'),source["product_sale_price"],latest_price))
                        es_client.update(index=hit["_index"],id=hit["_id"],doc={"product_sale_price":latest_price,"is_in_stock_updated_at":current_time,"is_in_stock":True,
                                                                                "updated_at": current_time})
                        logging.info("update success: " + str(source["product_id_orig"]))
                    else:
                        # 如果最新价格或激活状态不存在，将 Elasticsearch 中的数据标记为不在库存中
                        if latest_info.get("message") == STORE_CLOSED:
                            update_query = {
                                "script": {
                                    "source": """
                                        ctx._source.is_in_stock = params.is_in_stock;
                                        ctx._source.is_in_stock_updated_at = params.is_in_stock_updated_at;
                                    """,
                                    "lang": "painless",
                                    "params": {
                                        "is_in_stock": False,
                                        "is_in_stock_updated_at": current_time
                                    }
                                },
                                "query": {
                                    "term": {
                                        "product_id": product_id_for_matching
                                    }
                                }
                            }

                            # Perform the update
                            response = es_client.update_by_query(index="your_index", body=update_query)
                            logging.info(f'update all products not available in store {source["store_id"]} for loaction {hit["latitude"]},{hit["longitude"]} success: {response}')
                        else:
                            es_client.update(index=hit["_index"],id=hit["_id"],doc={"is_in_stock":False,"is_in_stock_updated_at":current_time})
                            logging.info("somthing wrong or product not exits: {}".format(latest_info))
                    # 在 Redis 中减少任务计数
                    r.decr(f'rtp_task:{index}:{product_id_for_matching}:count')
                    logging.debug("task size count: {}".format(r.get(f'rtp_task:{index}:{product_id_for_matching}:count')))
                except Exception as e:
                    # 在 Redis 中减少任务计数
                    traceback.print_exc()
                    r.decr(f'rtp_task:{index}:{product_id_for_matching}:count')
                    logging.debug("task size count: {}".format(r.get(f'rtp_task:{index}:{product_id_for_matching}:count')))

                        
        except Exception as e:
            logging.error(f"An error occurred at consume_messages:{e}")
            traceback.print_exc()

def monitor_tasks():
    while True:
        try:
            #获取所有以 "rtp_task:*:count" 模式匹配的键。这些键是存储每个产品实时价格检查任务剩余次数的 Redis 键。
            task_ids = r.keys("rtp_task:*:count")
            for task_id in task_ids:
                #获取任务的剩余次数
                remaining = r.get(task_id)
                #如果任务剩余为0
                if remaining == None or int(remaining) <= 0:
                    # real time price check is done for product_id
                    # 删除与任务相关的 Redis 键，清理任务状态。
                    r.delete(task_id)
                    # print(task_id,'done')
                    product_id = task_id.split(":")[2]
                    es_index = task_id.split(":")[1]
                    session.patch(url=f"{DB_UPDATE_API}/update_price_range",params={"product_id":product_id,"specific_index":es_index,"in_stock_only":True},headers={'accept':'application/json'})
                else:
                    time.sleep(0.1)
            if len(task_ids) == 0:
                time.sleep(0.1)
        except Exception as e:
            logging.error(f"monitor_tasks exception {e}")

if __name__ == '__main__':
    # TODO update store status (maybe not here but when user checkout cart)
    #线程的数量
    queue_names = [f"realtime_stock_{platform}_products" for platform in ["nn","is","tb","cf_now","kbs","cq"]]
    threads_for_each_platform = 5 # 1 or 5
    num_threads = threads_for_each_platform * len(queue_names) # 10 fixed number
    threads = []
    logging.info(f"redis real time price comsumer with threads {num_threads}")
    for i in range(num_threads):
        queue_name = queue_names[i % len(queue_names)]
        logging.info(f"queue_name {queue_name}")
        t = threading.Thread(target=consume_messages, args=(queue_name,))
        t.start()
        threads.append(t)
    #额外的监控线程
    monitor_thread = threading.Thread(target=monitor_tasks)
    monitor_thread.start()
    threads.append(monitor_thread)

    for t in threads:
        t.join()

