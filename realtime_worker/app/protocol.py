# -*- coding:utf-8 -*-
import json
import time
import uuid
import random
from urllib.parse import urlencode

from .network import Network


class Protocol(object):

    def __init__(self):
        self.network = Network()
        # self.proxies = {'https': 'http://127.0.0.1:8888', 'http': 'http://127.0.0.1:8888'}
        self.proxies = None

    def search_one(self, vendor_id, product_id):
        """
            talabat
        :return:
        """
        url = f'https://api.talabat.com/grocery/v1/vendors/{vendor_id}/products/{product_id}'
        params = {
            'brand': 'talabat',
            'country_code': 'ae',
            'vendor_id': vendor_id,
            'language_code': 'en-US'
        }
        url = url + '?' + urlencode(params)
        headers = {
            # 'x-newrelic-id': 'XQUPWFNbGwcBXVJRAgIGXg==',
            'accept': '*/*',
            'x-device-version': '9.6.0',
            'x-device-source': '4',
            'accept-language': 'en-US',
            'x-api-key': 'BlDRiUUJVDrpBgcN',
            'appbrand': '1',
            'brandtype': '1',
            'x-perseussessionid': '1679382257279.8503721460.wbwxcqryvs',
            'accept-encoding': 'gzip, deflate, br',
            'x-device-id': str(uuid.uuid4()),
            'x-perseusclientid': '1676432566543.5118696162.nazkpvgckh',
            'user-agent': 'Talabat/716 CFNetwork/1402.0.8 Darwin/22.2.0',
            'x-advertisingid': ''
        }
        return self.network.request(url=url, method='GET', headers=headers, proxies=self.proxies)

    def search_one_2(self, bid):
        url = f'https://api.talabat.com/menuapi/v2/branches/{bid}/menu'
        headers = {
            # 'x-newrelic-id': 'XQUPWFNbGwcBXVJRAgIGXg==',
            'accept': '*/*',
            'x-device-version': '9.6.0',
            'x-device-source': '4',
            'accept-language': 'en-US',
            'x-api-key': 'BlDRiUUJVDrpBgcN',
            'appbrand': '1',
            'brandtype': '1',
            'x-perseussessionid': '1679382257279.8503721460.wbwxcqryvs',
            'accept-encoding': 'gzip, deflate, br',
            'x-device-id': str(uuid.uuid4()),
            'x-perseusclientid': '1676432566543.5118696162.nazkpvgckh',
            'user-agent': 'Talabat/716 CFNetwork/1402.0.8 Darwin/22.2.0',
            'x-advertisingid': ''
        }
        return self.network.request(url=url, method='GET', headers=headers, proxies=self.proxies)

    def search_two(self, product_id, store_id):
        """
            instashop
        :return:
        """
        url = 'https://data.instashop.ae/parse/functions/getProductsAlternativesV2'
        data = {
            "ios": True,
            "productIdsToFetch": [product_id],
            "limit": 1000,
            "fetchCustomOptions": 2,
            "clientId": store_id,
            "showDisabledProducts": True,
            "page": 0,
            "android": False,
            "systemInfo": {
                "appVersion": "8.2.1",
                "lang": "en",
                "appBuild": 804,
                "platform": "ios",
                "countryId": "ryFmc6ACd1",
                "countryCode": "AE",
                "environment": "production",
                "timestamp": int(time.time()*1000),
                "idfa": "00000000-0000-0000-0000-000000000000"
            }
        }
        headers = {
            "x-parse-client-version": "i1.19.4",
            "content-type": "application/json; charset=utf-8",
            "accept": "*/*",
            "x-parse-session-token": "r:416a46d7eddd45a346cf29b32824643d",
            "x-parse-application-id": "Q8p0cZi0Es6POXNb4tNqqP7NdzXsqKd9Mzzdkdq6",
            "x-parse-client-key": "JeibBCaw0E5Mnpax37qA0Lh9DsNUmgm0kZZxlJSV",
            "x-parse-installation-id": "091e3e18-b0bf-4006-9d60-4123a057b6e9",
            "x-parse-os-version": "16.2 (20C65)",
            "accept-language": "zh-CN,zh-Hans;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "user-agent": "InstaShop/804 CFNetwork/1402.0.8 Darwin/22.2.0",
            "x-parse-app-build-version": "804",
            "x-parse-app-display-version": "8.2.1"
        }
        return self.network.request(url=url, method="POST", data=json.dumps(data, separators=(',', ':')),
                                    headers=headers, proxies=self.proxies)

    def search_three(self, product_family, product_name, lat, lng, page_number=1):
        """
            nownow
        :return:
        """
        url = 'https://api-app-basket.noonnownow.com/search/external/api/v1/search/global'
        data = {
            "item_search_pagination": {
                "page_number": page_number,
                "product_family": product_family
            },
            "search_key": product_name,
            "store_search_pagination": {"from": 1},
            "user": {"lat": lat, "lon": lng},
            "search_context": "SEARCH_BAR"
        }
        headers = {
            "accept": "application/json, text/plain, */*",
            "x-tenant": "basket",
            "x-device-id": "".join([random.choice('abcdef0123456789') for i in range(16)]),
            "x-version": "1.72",
            "x-country-code": "UAE",
            "x-language": "en",
            "x-app-token": "",
            # "x-app-token": "eKqhoWRaRZK22a-HHLslKd:APA91bHA61vMfNtlF2h2WunVb8IEkEOvUw3ZGS4YFAkl5IJubdNR-ziUE8V"
            #                "LOs_wEFYMLfHS08H3jGfzyIkYiCbBYrfNkom8RcEqeQ9J1B2hsLBDkJDhZ3AlhbIVIfrgSm3ifN_2DN5v",
            "x-locale": "en-AE",
            "content-type": "application/json",
            "accept-encoding": "gzip",
            "user-agent": "okhttp/3.12.12"
        }
        return self.network.request(url=url, method='POST', data=json.dumps(data, separators=(',', ':')),
                                    headers=headers, proxies=self.proxies)

    def search_four(self, product_name, lat, lng, delivery_time='STANDARD', page_number=0):
        """
            carrefour
        :param delivery_time: 配送时长: "Now" or "STANDARD"
        :return:
        """
        url = "https://api-prod.retailsso.com/v2/search/mafuae/en/search"
        params = {
            "keyword": product_name,
            "filter": "",
            "sortBy": "relevance",
            "currentPage": page_number,
            "pageSize": "200",  # default 40
            "maxPrice": "",
            "minPrice": "",
            "latitude": lat,
            "longitude": lng,
            "lang": "en",
            "nextOffset": "0",
            "disableSpellCheck": "false",
            "asgCategoryId": "",
            "asgCategoryName": "",
            "requireSponsProducts": "true",
            "forceRefresh": "false",
            "needVariantsData": "true"
        }
        url = url + '?' + urlencode(params)
        headers = {
            "intent": delivery_time,
            "service": "algolia",
            "newrelic": "eyJ2IjpbMCwyXSwiZCI6eyJkLnR5IjoiTW9iaWxlIiwiZC5hYyI6IjMzNTU3MjAiLCJkLmFwIjoiMT"
                        "EzMzkwODg3NiIsImQudHIiOiJiYzlhZjg3NGYzODY0YjhhYWMwMTVlNTkwNmEyYjI3OCIsImQuaWQi"
                        "OiIxYjdhYmFjNDFiYmU0Zjg0IiwiZC50aSI6MTY4MDQxMzA5ODQ4MH19",
            "tracestate": "@nr=0-2-3355720-1133908876-----1680413098480",
            "traceparent": "00-bc9af874f3864b8aac015e5906a2b278--00",
            "appversion": "313",
            "env": "PROD",
            "langcode": "en",
            "storeid": "mafuae",
            "userid": "anonymous",
            "deviceid": "mVOZ6DZpdkVOOYybAwoqXw==",
            "appid": "Android",
            "osversion": "27",
            "token": "g_XMsUuF9wH6kTV03SzVwNcNsM0",
            "accept-encoding": "gzip",
            "user-agent": "okhttp/4.9.3",
            "x-newrelic-id": "VwUCVFFRCBABVVJRDgEPXlMH"
        }
        return self.network.request(url=url, method='GET', headers=headers, proxies=self.proxies)

    def search_store_one(self, store_name, lat, lng):
        url = 'https://api.talabat.com/qcommerce/search/api/v1/vendor'
        data = {
            "latitude": lat,
            "longitude": lng,
            "query": store_name,
            "limit": 100,
            "country_id": 4,
            "only_open_vendors": False,
            "filter_ids": [],
            "filters": [],
            "cuisine_id": 0,
            "collection_id": 0,
            "rank_vendors": True,
            "page_number": 0,  # TODO: Consider turn the page
            "ignore_migrated_groceries": False,
            "enable_tgo_rank": False,
            "search_config": "",
            "use_nfv_ranking": False,
            "vertical_ids": [1],
            "is_tpro_user": False
        }
        headers = {
            "x-perseussessionid": "1680943738207.1319992827.vekbmsznjb",
            "user-agent": "Dart/2.15 (dart:io)",
            "x-device-version": "9.7.8",
            "x-device-source": "4",
            "x-perseusclientid": "1680694738786.3279244247.vfmtgabqit",
            "accept-encoding": "gzip",
            "x-requestid": "7beaf520-d5eb-11ed-b7b0-e3cc8b0b3410",
            "content-type": "application/json; charset=utf-8",
            "appbrand": "1",
            "accept-language": "en-US",
            "host": "api.talabat.com",
            "x-device-id": str(uuid.uuid4())
        }
        return self.network.request(url=url, method='POST', headers=headers,
                                    data=json.dumps(data, separators=(',', ':')), proxies=self.proxies)

    def search_store_two(self, store_id, lat, lng):
        url = 'https://data.instashop.ae/parse/functions/client_status'
        data = {
            "language": "en",
            "fetchClosedMsg": True,
            "clientId": store_id,
            "latitude": lat,
            "longitude": lng,
            "systemInfo": {
                "appVersion": "8.3.0",
                "lang": "en",
                "appBuild": 806,
                "platform": "ios",
                "countryId": "ryFmc6ACd1",
                "LVC": "G9rw3F20le",
                "countryCode": "AE",
                "environment": "production",
                "timestamp": int(time.time() * 1000),
                "idfa": "00000000-0000-0000-0000-000000000000"
            }
        }
        headers = {
            "x-parse-client-version": "i1.19.4",
            "content-type": "application/json; charset=utf-8",
            "accept": "*/*",
            "x-parse-session-token": "r:d0c3a6cf2626690b552a3ab4f6667f2f",
            "x-parse-application-id": "Q8p0cZi0Es6POXNb4tNqqP7NdzXsqKd9Mzzdkdq6",
            "x-parse-client-key": "JeibBCaw0E5Mnpax37qA0Lh9DsNUmgm0kZZxlJSV",
            "x-parse-installation-id": "1fe9aff2-62b7-40b4-9239-2c755578909c",
            "x-parse-os-version": "14.0.1 (18A393)",
            "accept-language": "zh-cn",
            "accept-encoding": "gzip, deflate, br",
            "user-agent": "InstaShop/806 CFNetwork/1197 Darwin/20.0.0",
            "x-parse-app-build-version": "806",
            "x-parse-app-display-version": "8.3.0"
        }
        return self.network.request(url=url, method='POST', data=json.dumps(data, separators=(',', ':')),
                                    headers=headers, proxies=self.proxies)

    def search_store_three(self, store_id, lat, lng):
        url = "https://api-app-basket.noonnownow.com/store-service/external/v1/store"
        params = {
            "lat": lat,
            "lng": lng,
            "store-id": store_id,
            "message-context": "NEARBY"
        }
        url = url + '?' + urlencode(params)
        headers = {
            "accept": "application/json, text/plain, */*",
            "x-tenant": "basket",
            "x-device-id": "".join([random.choice('abcdef0123456789') for i in range(16)]),
            "x-version": "1.72",
            "x-country-code": "UAE",
            "x-language": "en",
            "x-app-token": "",
            "x-locale": "en-AE",
            "accept-encoding": "gzip"
        }
        return self.network.request(url=url, method='GET', headers=headers, proxies=self.proxies)