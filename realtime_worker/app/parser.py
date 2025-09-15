# -*- coding:utf-8 -*-
import json
import logging

logger = logging.getLogger(__name__)


class Parser(object):

    def __init__(self):
        self.status_value_dict = {'0': 'Normal', '1': 'Closed', '2': 'Busy'}

    @staticmethod
    def parse_search_one(content, vendor_id):
        """
            talabat shop_type=1
        :return:
        """
        if not content:
            return False, 'ErrorNet'

        if isinstance(content, bytes):
            content = content.decode()

        try:
            content = json.loads(content)
            data = content['data']
            item = {
                'platform': 'talabat',
                'vendor_id': vendor_id,
                'product_id': data['id'],
                'product_name': data['name'],
                'product_price': data['price'],
                'product_stock_amount': data['stock_amount'],
                'product_active': True if data['stock_amount'] > 0 else False
            }
            return True, item
        except Exception as e:
            logger.error('Parser search one exception occurred, err={}'.format(e), exc_info=True)
        return False, 'ErrorException'

    @staticmethod
    def parse_search_one_2(content, product_id):
        """
            talabat shop_type=1
        :return:
        """
        if not content:
            return False, 'ErrorNet'

        if isinstance(content, bytes):
            content = content.decode()

        try:
            content = json.loads(content)
            menu_section = content['result']['menu']['menuSection']
            for menu in menu_section:
                for _ in menu['itm']:
                    if str(_['id']) != product_id:
                        continue
                    item = {
                        'platform': 'talabat',
                        'store_id': None,
                        'product_id': product_id,
                        'product_name': _['nm'],
                        'product_price': _['pr'],
                        'product_stock_amount': None,
                        'product_active': True  # default
                    }
                    return True, item
            return False, 'The specified product was not found'
        except Exception as e:
            logger.error('Parser search one exception occurred, err={}'.format(e), exc_info=True)
        return False, 'ErrorException'

    @staticmethod
    def parse_search_two(content, product_id):
        """
            instashop
        :return:
        """
        if not content:
            return False, 'ErrorNet'

        if isinstance(content, bytes):
            content = content.decode()

        try:
            content = json.loads(content)
            product = content['result']['products'][0]

            clients = product['productObject']['clients']
            key = list(clients.keys())[0]

            item = {
                'platform': 'instashop',
                'product_id': product_id,
                'store_id': key,
                'product_name': product['title'],
                'product_price': clients[key]['price']['retail'],
                'product_active': clients[key]['active']
            }
            return True, item
        except Exception as e:
            logger.error('Parser search two exception occurred, err={}'.format(e), exc_info=True)
        return False, 'ErrorException'

    @staticmethod
    def parse_search_three(content, store_id, product_id):
        """
            nownow
        :return:
        """
        if not content:
            return False, 'ErrorNet'

        if isinstance(content, bytes):
            content = content.decode()

        try:
            content = json.loads(content)
            store_item_search_results = content['bucketed_items_search_result']['store_item_search_results']
            is_end = False if content['bucketed_items_search_result'].get('next_store_id') else True
            for store_item in store_item_search_results:
                if store_id != store_item['store']['store_id']:
                    continue
                item_search_results = store_item['item_search_results']
                for item_search_result in item_search_results:
                    if product_id != item_search_result['item_id']:
                        continue
                    item = {
                        'platform': 'nownow',
                        'product_id': product_id,
                        'product_name': item_search_result['title'],
                        'product_price': item_search_result['sale_price'],
                        'product_active': True
                    }
                    return True, item
            if is_end:
                return False, 'The specified product was not found'
            return False, False
        except Exception as e:
            logger.error('Parser search three exception occurred, err={}'.format(e), exc_info=True)
        return False, 'ErrorException'

    @staticmethod
    def parse_search_four(content, product_id, delivery_time):
        """
            carrefour
        :return:
        """
        if not content:
            return False, 'ErrorNet'

        if isinstance(content, bytes):
            content = content.decode()

        try:
            content = json.loads(content)

            status_code = content['meta']['statusCode']
            if status_code != 200:
                return False, content['meta']['message']

            products = content['data']['products']
            total_products = content['data']['totalProducts']
            is_end = False if total_products > len(products) else True
            for product in products:
                if product_id != product['id']:
                    continue

                item = {
                    'platform': 'carrefour',
                    'product_id': product_id,
                    'product_name': product['name'],
                    'product_price': product['price']['price'],
                    'product_active': True if product['stock']['stockLevelStatus'] == 'inStock' else False,
                    'delivery_time': delivery_time
                }
                return True, item
            if is_end:
                return False, 'The specified product was not found'
            return False, False
        except Exception as e:
            logger.error('Parser search four exception occurred, err={}'.format(e), exc_info=True)
        return False, 'ErrorException'

    def parse_search_store_one(self, content, store_id):
        if not content:
            return False, 'ErrorNet'

        if isinstance(content, bytes):
            content = content.decode()

        try:
            content = json.loads(content)
            error = content['error']
            if error:
                return False, error

            result = content['result']
            for res in result:
                if store_id != res['chain_id']:
                    continue
                item = {
                    # rating,is_dark_store,is_talabat_pro delivery_time delivery_charges
                    'platform': 'talabat',
                    'store_id': store_id,
                    'store_name': res['vendor_name'],
                    'store_status': self.status_value_dict.get(str(res['vendor_status'])),
                    'store_status_value': res['vendor_status'],
                    'rating':res['rating'],
                    'is_dark_store':res['is_dark_store'],
                    'is_talabat_pro':res['is_talabat_pro'],
                    'delivery_time':res['delivery_time'],
                    'delivery_charges':res['delivery_charges'],
                    'store_status_message': None
                }
                return True, item
            return False, 'The specified store was not found'
        except Exception as e:
            logger.error('Parser search store one exception occurred, err={}'.format(e), exc_info=True)
        return False, 'ErrorException'

    def parse_search_store_two(self, content, store_id, store_name):
        # '{"result":{"info":{"deliveryCharges":5.25,"minimumOrder":30,"minimumOrderFee":0,"deliveryChargesText":null,"workload":0.3,"invoice":false},
        # "operationalStatus":{"closed":false,"instantService":{"enabled":true,"deliveryTime":25}},"externalDeliveryService":[null]}}'
        # deliveryCharges
        # minimumOrder
        # instantService. enabled deliveryTime
        if not content:
            return False, 'ErrorNet'

        if isinstance(content, bytes):
            content = content.decode()

        try:
            content = json.loads(content)
            operational_status = content['result']['operationalStatus']
            status_value = 1 if operational_status['closed'] else 0
            status_message = operational_status.get('closedMsgBasket')
            item = {
                'platform': 'instashop',
                'store_id': store_id,
                'store_name': store_name,
                'store_status': self.status_value_dict.get(str(status_value)),
                'store_status_value': status_value,
                'store_status_message': status_message,
                'delivery_charges':content['result'].get('info',{}).get("deliveryCharges"),
                'delivery_time':operational_status.get('instantService',{}).get("deliveryTime"),
                'instant_service_enabled':operational_status.get('instantService',{}).get("enabled"),
                'minimum_order':content['result'].get('info',{}).get("minimumOrder"),
            }
            return True, item
        except Exception as e:
            logger.error('Parser search store two exception occurred, err={}'.format(e), exc_info=True)
        return False, 'ErrorException'

    def parse_search_store_three(self, content, store_id):
        if not content:
            return False, 'ErrorNet'

        if isinstance(content, bytes):
            content = content.decode()

        try:
            content = json.loads(content)
            store_serviceability = content['store_serviceability']
            serviceable = store_serviceability['serviceable']
            store_status_value = 0 if serviceable else 1
            store_status_message = store_serviceability.get('message')
            item = {
                'platform': 'nownow',
                'store_id': store_id,
                'store_name': content['name'],
                'store_status': self.status_value_dict.get(str(store_status_value)),
                'store_status_value': store_status_value,
                'store_status_message': store_status_message
            }
            return True, item
        except Exception as e:
            logger.error('Parser search store three exception occurred, err={}'.format(e), exc_info=True)
        return False, 'ErrorException'
