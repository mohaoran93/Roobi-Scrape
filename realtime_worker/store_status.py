from elasticsearch import Elasticsearch
from dotenv import dotenv_values
import sys
import logging
import h3
import requests
from datetime import datetime
import random
import time
import uuid
import json

sys.path.append("/home/haoran/projects/roobi-scraper/platforms")
sys.path.append("/home/haoran/projects/roobi-scraper/platforms/carrefour/scrap_realtime_price")
from nownow.call_x_bytes import nownow_store_details,nownow_list_stores

config = dotenv_values("realtime_worker/.env")
es_host = config["ES_HOST"]
NOWNOW_STORE_CLOSED_TEXT = "unable to accept new orders"
STORE_CLOSED = "store closed"
STORE_OPEN = "store open"
STORE_NOTFOUND = "store not found"

class StoreStatus():
    def __init__(self):
        self.store_status_index = "roobi_store_status"
        self.store_status_cache_time = 15 # 15 # mintues
        ELASTIC_PASSWORD=config["ELASTIC_PASSWORD"]
        self.es_client = Elasticsearch(es_host,
            basic_auth=("elastic", ELASTIC_PASSWORD)
        )

        if not self.es_client.indices.exists(index=self.store_status_index):
            mapping = {
                "mappings": {
                    "properties": {
                        "store_id": {"type": "keyword"},
                        "store_name": {"type": "text"},
                        "store_status": {"type": "keyword"},
                        "store_status_updated_at": {"type": "date"},
                        "store_status_valid_until": {"type": "date"}, # future use, to have dynamtic cache time
                        "platform": {"type": "keyword"},
                        "h3_cell": {"type": "keyword"}
                    }
                }
            }
            self.es_client.indices.create(index=self.store_status_index, body=mapping)

        self.instashop_business_type = {
            'Supermarkets': 'grocery',
            # 'Restaurants': 'food', #  skip restaurants, restaurants has more 50 stores, need pagination, and we don't care restaurants for now.
            'Pharmacies': 'pharmacy',
            # 'Bakeries & Cakes': 'bakery',
            'Pet Shops': 'petshop',
            'Butchery & BBQ': 'butchery',
            'Fruits & Vegetables': 'fruitsAndVegetables',
            'Seafood': 'seafood',
            'Cosmetics & Beauty': 'cosmeticsAndBeauty',
            # 'Flower Shops': 'flowershop','Stationery': 'stationery','Perfumes': 'fragrances','Specialty & Ethnic': 'specialtyShop',
            'Water': 'water',
            'Organic Shops': 'organic',
            #'Fitness Nutrition': 'nutrition','Home & Living': 'homeImprovements','Electronics': 'electronics','Giving Back': 'donation','Coffee & Tea Corner': 'coffeeCapsMachines','Book a house cleaning': 'cleaner','seasonal':'seasonal','gamesAndToys':'gamesAndToys',
            }
        self.instashop_system_info = {
            'lang': 'en',
            'appBuild': 744,
            'platform': 'ios',
            'countryId': 'ryFmc6ACd1',
            'countryCode': 'AE',
            'environment': 'production',
            'idfa': '00000000-0000-0000-0000-000000000000'
        }

        self.instashop_headers = {
            'x-parse-client-version': 'i1.19.3',
            'content-type': 'application/json; charset=utf-8',
            'accept': '*/*',
            'x-parse-session-token': 'r:49bb17b81d255f2c3ad6a1f4d6a97bb3',
            'x-parse-application-id': 'Q8p0cZi0Es6POXNb4tNqqP7NdzXsqKd9Mzzdkdq6',
            'x-parse-client-key': 'JeibBCaw0E5Mnpax37qA0Lh9DsNUmgm0kZZxlJSV',
            'x-parse-installation-id': 'ce272778-7f53-437d-8df3-5978e688d618',
            'x-parse-os-version': '15.3.1 (19D52)',
            'accept-language': 'zh-CN,zh-Hans;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'user-agent': 'InstaShop/744 CFNetwork/1329 Darwin/21.3.0',
            'x-parse-app-build-version': '744',
            'x-parse-app-display-version': '7.4.0'
        }

    def update_store_status(self, store_id, store_name, store_status, platform,lat,lng):
        doc = {
            "store_id": store_id,
            "store_name": store_name,
            "store_status": store_status,
            "store_status_updated_at": "now",
            "platform": platform
        }
        self.es_client.index(index=self.store_status_index, body=doc)
    def get_status_from_cache(self, store_id, platform,lat,lng,h3_cell=None):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"h3_cell": h3_cell}},
                        {"match": {"store_id": store_id}},
                        {"match": {"platform": platform}},
                        {"range": {"store_status_updated_at": {"gte": "now-{}m".format(self.store_status_cache_time)}}}
                    ]
                }
            }
        }
        res = self.es_client.search(index=self.store_status_index, body=query)
        if res["hits"]["total"]["value"] > 0:
            store_info = res["hits"]["hits"][0]
            return {'code': 200, 'store_status': store_info["_source"]["store_status"], 'info_source':"cache"}
        else:
            return None
    def get_store_status(self, store_id, platform,lat,lng,h3_cell=None):
        h3_cell = h3.geo_to_h3(float(lat),float(lng), 10) if h3_cell is None else h3_cell
        current_time = datetime.utcnow().isoformat()
        # step 1 try to use cache first
        cache = self.get_status_from_cache(store_id, platform,lat,lng,h3_cell)
        if cache:
            return cache
        
        # step 2 get real time store status
        if platform == "nownow":
            # TODO test the function
            _id = "{}_{}_{}".format(h3_cell,store_id,platform)
            # list all stores
            nownow_list_stores_res = nownow_list_stores(latitude=lat, longitude=lng)
            for store_details in nownow_list_stores_res.get("results",{}).get("data",{}).get("stores",[]):
                target_store_status = STORE_NOTFOUND
                hours_detail_text = store_details.get("results",{}).get("data",{}).get('store_details',{}).get('Store_hour_details',STORE_NOTFOUND)

                if hours_detail_text == STORE_NOTFOUND:
                    # store not found
                    self.es_client.index(index=self.store_status_index,id=_id, body={"store_id": store_id, "store_name": store_details.get('Store_Name'), \
                                                                                "store_status": STORE_NOTFOUND, "store_status_updated_at": current_time, "platform": platform,"h3_cell":h3_cell})
                    if store_details.get("Store_Code") == store_id:
                        logging.debug(f"nownow real time store details response {store_details}")
                        target_store_status = STORE_NOTFOUND

                if NOWNOW_STORE_CLOSED_TEXT in hours_detail_text:
                    # store closed
                    self.es_client.index(index=self.store_status_index,id=_id, body={"store_id": store_id, "store_name": store_details.get('Store_Name'), \
                                                                                "store_status": STORE_CLOSED, "store_status_updated_at": current_time, "platform": platform,"h3_cell":h3_cell})
                    if store_details.get("Store_Code") == store_id:
                        logging.debug(f"nownow real time store details response {store_details}")
                        target_store_status = STORE_CLOSED
                else:
                    # store open
                    self.es_client.index(index=self.store_status_index,id=_id, body={"store_id": store_id, "store_name": store_details.get('Store_Name'), \
                                                                                "store_status": STORE_OPEN, "store_status_updated_at": current_time, "platform": platform,"h3_cell":h3_cell})
                    if store_details.get("Store_Code") == store_id:
                        logging.debug(f"nownow real time store details response {store_details}")
                        target_store_status = STORE_OPEN
            return {'code': 200, 'store_status': target_store_status,'info_source':"realtime"}
                            
        if platform == "instashop":
            lat,lng=float(lat),float(lng)
            url = 'https://data.instashop.ae/parse/functions/fast_areaClients'
            store_status_res = {'code': 200, 'store_status': STORE_NOTFOUND,'info_source':"did not find store"}
            for department in self.instashop_business_type.keys():
                skip = 0
                data = {
                    'systemInfo': self.instashop_system_info,
                    'businessTypeTypes': [self.instashop_business_type[department]],
                    'limit': 50,
                    'areas': None,
                    'skip': skip,
                    'coordinates': {'__type': 'GeoPoint', 'longitude': lng, 'latitude': lat}
                }
                res = requests.post(url=url,json=data,headers=self.instashop_headers).json()
                clients = res['result']['clients']
                if len(clients) >= 50:
                    logging.warning("first_category_by_store department {} may has more stores total clients {}".format(department,len(clients))) # TODO if has more, we did not handle it for now
                for client in clients:
                    store_info = {                                                
                            "store_status_updated_at": current_time,
                            "platform": "instashop",
                            "h3_cell": h3_cell,
                            'store_name': client['name'],
                            'store_status': STORE_CLOSED if client['client_status']['operationalStatus']['closed'] else STORE_OPEN,
                            'store_id': client['objectId'],
                        }
                    self.es_client.index(index=self.store_status_index,id="{}_{}_{}".format(h3_cell,client['objectId'],"instashop"), body=store_info)
                    if client['objectId'] == store_id:
                        store_status_res = {'code': 200, 'store_status': store_info['store_status'],'info_source':"realtime"}      

            if store_status_res['store_status'] == STORE_NOTFOUND:
                store_info = {                                                
                            "store_status_updated_at": current_time,
                            "platform": "instashop",
                            "h3_cell": h3_cell,
                            'store_name': "store not found",
                            'store_status': STORE_NOTFOUND,
                            'store_id': store_id,
                        }
                self.es_client.index(index=self.store_status_index,id="{}_{}_{}".format(h3_cell,store_id,"instashop"), body=store_info)    
            return store_status_res
        if platform == "talabat":
            # step 1 get area id
            country_id = '4'
            url_1 = 'https://api.talabat.com/api/v1/apps/googlearea/{}/{}/{}'.format(lat, lng,country_id)
            headers = {
                "accept": "*/*",
                "x-device-version": "9.6.0",
                "x-device-source": "4",
                "accept-language": "en-US",
                "brandtype": "1",
                "accept-encoding": "gzip",
                "appbrand": "1",
                "x-perseussessionid": "{}.9796705296.gytxnrjvxr".format(int(time.time()*1000)),
                "user-agent": "Talabat/9.6.0 (iPhone; iOS 16.2; Scale/3.00)",
                "x-device-id": str(uuid.uuid4()).upper(),
                "x-perseusclientid": "{}543.5118696162.nazkpvgckh".format(1676432566+random.randint(-4000, 4000)),
                "Referer": None
            }
            res1 = requests.get(url_1,headers=headers)
            area_id = res1.json()['result']['area']['id']
            print(res1.json())
            # step 2
            url_2 = "https://api.talabat.com/discovery/v1/ae/qcommerce/swimlanes"
            params = {
                "areaId": area_id,
                "lat": lat,
                "lon": lng,
                "vertical_ids": "1",
                "swimlane_config": "grocery-v2",
                "ranking_weight_set": "Reliability-v2"
            }
            headers = {
            "user-agent": "Dart/2.15 (dart:io)",
            "x-perseussessionid": "{}.9796705296.gytxnrjvxr".format(int(time.time()*1000)),
            "x-device-version": "9.6.0",
            "x-device-source": "4",
            "accept-language": "en-US",
            "x-perseusclientid": "{}543.5118696162.nazkpvgckh".format(1676432566+random.randint(-4000, 4000)),
            "appbrand": "1",
            "accept-encoding": "gzip",
            "x-device-id": str(uuid.uuid4()).upper(),
            "host": "api.talabat.com",
            "Referer": None
            }
            res2 = requests.get(url_2,params=params,headers=headers).json()
            # print(res2.json())
            swim_lanes = res2['swimlanes']
            # step 3
            store_status_res = {'code': 200, 'store_status': STORE_NOTFOUND,'info_source':"did not find store"}
            for swim_lane in swim_lanes:
                vendors = swim_lane['vendors']
                # save vendors to file
                for vendor in vendors:
                    store_info = {                                                
                            "store_status_updated_at": current_time,
                            "platform": "talabat",
                            "h3_cell": h3_cell,
                            'store_name': vendor['na'],
                            'store_status': STORE_CLOSED if vendor['stt'] == 1 else STORE_OPEN,
                            'store_id': vendor['id'],
                        }
                    self.es_client.index(index=self.store_status_index,id="{}_{}_{}".format(h3_cell,vendor['id'],"talabat"), body=store_info)
                    if vendor['id'] == store_id:
                        store_status_res = {'code': 200, 'store_status': store_info['store_status'],'info_source':"realtime"}
            if store_status_res['store_status'] == STORE_NOTFOUND:
                store_info = {                                                
                            "store_status_updated_at": current_time,
                            "platform": "talabat",
                            "h3_cell": h3_cell,
                            'store_name': "store not found",
                            'store_status': STORE_NOTFOUND,
                            'store_id': store_id,
                        }
                self.es_client.index(index=self.store_status_index,id="{}_{}_{}".format(h3_cell,store_id,"talabat"), body=store_info)
            return store_status_res
                    

if __name__ == "__main__":
    store_status = StoreStatus()
    # test data from products_geo_info_8a43a1301a97fff_2024052705
    
    # test nownow store status
    # res = store_status.get_store_status("WFLWRSNKPK","nownow",25.08674796956855,55.14782017460913)
    # print(res)
    
    # test instashop store status
    res = store_status.get_store_status("ETXJEsdVwU","instashop","25.08674796956855","55.14782017460913")
    print(res)
    # res = store_status.get_store_status("fskti","instashop",25.08674796956855,55.14782017460913)
    # print(res)
    # res = store_status.get_store_status("AAwDZQ03gc","instashop",25.08674796956855,55.14782017460913)
    # print(res)
    # res = store_status.get_store_status("SJ7AWvvLVA","instashop",25.08674796956855,55.14782017460913)
    # print(res)

    # test talabat store status
    # res = store_status.get_store_status("619472","talabat",25.08674796956855,55.14782017460913)
    # print(res)
    # res = store_status.get_store_status("637375","talabat",25.08674796956855,55.14782017460913)
    # print(res)
    # res = store_status.get_store_status("652045","talabat",25.055831461581512, 55.28532350001956)
    # print(res)
    # res = store_status.get_store_status("641943","talabat",25.055831461581512, 55.28532350001956)
    # print(res)
    # res = store_status.get_store_status("641944","talabat",25.055831461581512, 55.28532350001956)
    # print(res)
    
