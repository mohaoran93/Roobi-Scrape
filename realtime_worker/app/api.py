# -*- coding:utf-8 -*-
import logging

from .protocol import Protocol
from .parser import Parser

logger = logging.getLogger(__name__)


class Api(object):

    def __init__(self):
        self.protocol = Protocol()
        self.parser = Parser()

    def search_one(self, vendor_id, product_id, shop_type, bid):
        """
            tabalat
        :return:
        """
        if shop_type == 1:
            content = self.protocol.search_one(vendor_id=vendor_id, product_id=product_id)
            status, message = self.parser.parse_search_one(content=content, vendor_id=vendor_id)
        elif shop_type == 2:
            content = self.protocol.search_one_2(bid=bid)
            print(content)
            status, message = self.parser.parse_search_one_2(content=content, product_id=product_id)
        else:
            data = {
                'platform': 'talabat',
                'vendor_id': vendor_id,
                'product_id': product_id,
                'shop_type': shop_type,
                'bid': bid
            }
            return {'code': 500, 'message': 'shop type value is invalid.', 'data': data}

        if status:
            return {'code': 200, 'message': 'success', 'data': message}

        data = {
            'platform': 'talabat',
            'vendor_id': vendor_id,
            'product_id': product_id,
            'shop_type': shop_type,
            'bid': bid
        }
        return {'code': 500, 'message': message, 'data': data}

    def search_two(self, product_id, store_id):
        """
            instashop
        :return:
        """
        content = self.protocol.search_two(product_id=product_id, store_id=store_id)
        status, message = self.parser.parse_search_two(content=content, product_id=product_id)
        # message is actualy data when status is True
        if status:
            return {'code': 200, 'message': 'success', 'data': message}

        data = {
            'platform': 'instashop',
            'product_id': product_id
        }
        return {'code': 500, 'message': message, 'data': data}

    def search_three(self, product_family, product_name, product_id, store_id, lat, lng):
        """
            nownow
        :return:
        """
        page_number = 1
        # is_end = False
        # while not is_end:
        content = self.protocol.search_three(product_family=product_family, product_name=product_name,
                                                page_number=page_number, lat=lat, lng=lng)

        status, message = self.parser.parse_search_three(content=content, store_id=store_id, product_id=product_id)
        data = {
                'platform': 'nownow',
                'product_id': product_id,
                'product_family': product_family,
                'product_name': product_name,
                'store_id': store_id
            }
        if status:
            return {'code': 200, 'message': 'success', 'data': message}

        elif isinstance(message, str):
            
            return {'code': 500, 'message': message, 'data': data}
        else:
            return {'code': 500, 'message': "nownow other issue", 'data': data}

            # page_number += 1

    def search_four(self, product_name, product_id, lat, lng, delivery_time):
        """
            carrefour
        :return:
        """
        page_number = 0
        # is_end = False
        # while not is_end:
        content = self.protocol.search_four(product_name=product_name, lat=lat, lng=lng,
                                            page_number=page_number, delivery_time=delivery_time)
        status, message = self.parser.parse_search_four(content=content, product_id=product_id,
                                                        delivery_time=delivery_time)
        data = {
                'platform': 'carrefour',
                'product_id': product_id,
                'product_name': product_name,
                'lat': lat,
                'lng': lng,
                'delivery_time': delivery_time
            }
        if status:
            return {'code': 200, 'message': 'success', 'data': message}

        elif isinstance(message, str):
            
            return {'code': 500, 'message': message, 'data': data}
        else:
            return {'code': 500, 'message': "carrefour other issue", 'data': data}

            # page_number += 1

    def search_store_one(self, store_name, store_id, lat, lng):
        """
            talabat
        :param lng:
        :param lat:
        :param store_name:
        :param store_id:
        :return:
        """
        content = self.protocol.search_store_one(store_name=store_name, lat=lat, lng=lng)
        # print(f"content {content}")
        status, message = self.parser.parse_search_store_one(content=content, store_id=store_id) # rating,is_dark_store,is_talabat_pro delivery_time delivery_charges# what is is_migrated,
        if status:
            return {'code': 200, 'message': 'success', 'data': message}

        data = {
            'platform': 'talabat',
            'store_name': store_name,
            'store_id': store_id,
            'lat': lat,
            'lng': lng
        }
        return {'code': 500, 'message': message, 'data': data}

    def search_store_two(self, store_name, store_id, lat, lng):
        """
            instashop
        :param lng:
        :param lat:
        :param store_name:
        :param store_id:
        :return:
        """
        content = self.protocol.search_store_two(store_id=store_id, lat=lat, lng=lng)
        # print(f"content {content}")
        status, message = self.parser.parse_search_store_two(content=content, store_id=store_id, store_name=store_name)
        if status:
            return {'code': 200, 'message': 'success', 'data': message}

        data = {
            'platform': 'instashop',
            'store_name': store_name,
            'store_id': store_id,
            'lat': lat,
            'lng': lng
        }
        return {'code': 500, 'message': message, 'data': data}

    def search_store_three(self, store_name, store_id, lat, lng):
        """
            nownow
        :param lng:
        :param lat:
        :param store_name:
        :param store_id:
        :return:
        """
        content = self.protocol.search_store_three(store_id=store_id, lat=lat, lng=lng)
        print(f"content {content}")
        # TODO 
        """'{"store_id":"1778","name":"Union Coop","images_url":[],"distance":92.9,"distance_unit":"km","eta":{"estimated_time":55,"time_unit":"mins"},
        "store_serviceability":{"address_id":0,"unserviceable_reason_type":"STORE_CLOSED","message":".",
        },"area":"Al Barsha 3","polygon_coordinates":[{"lat":25.105316416053235,"lng":55.17654490899852},],"min_order_value":25.00}'"""
        status, message = self.parser.parse_search_store_three(content=content, store_id=store_id)
        if status:
            return {'code': 200, 'message': 'success', 'data': message}

        data = {
            'platform': 'nownow',
            'store_name': store_name,
            'store_id': store_id,
            'lat': lat,
            'lng': lng
        }
        return {'code': 500, 'message': message, 'data': data}

