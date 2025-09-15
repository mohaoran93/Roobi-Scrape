# -*- coding:utf-8 -*-
import logging
import time
import random

import requests
import urllib3
urllib3.disable_warnings()

logger = logging.getLogger(__name__)


class Network(object):

    def __init__(self):
        self.session = requests.session()
        self.session.headers.clear()
        self.session.cookies.clear()

    def request(self, url, method='GET', data=None, headers=None, cookies=None, proxies=None, retry=2):
        if headers:
            self.session.headers.clear()
            self.session.headers.update(headers)
        if cookies:
            self.session.cookies.clear()
            self.session.cookies.update(cookies)

        for i in range(retry):
            try:
                if method == 'GET':
                    response = self.session.get(url=url, proxies=proxies, timeout=(5, 10), verify=False)
                else:
                    response = self.session.post(url=url, data=data, proxies=proxies, timeout=(5, 10), verify=False)
                if response.status_code == 200 or response.status_code == 204:
                    return response.content
                else:
                    logger.warning(f'status code is {response.status_code} content is {response.json()}')

            except requests.exceptions.ConnectTimeout:
                logger.warning('requests.exceptions.ConnectTimeout')
            except requests.exceptions.ReadTimeout:
                logger.warning('requests.exceptions.ReadTimeout')
            except requests.exceptions.Timeout:
                logger.warning('requests.exceptions.Timeout')
            except requests.exceptions.ProxyError:
                logger.warning('requests.exceptions.ProxyError')
                time.sleep(random.uniform(3, 5))
            except requests.exceptions.ConnectionError:
                logger.warning('requests.exceptions.ConnectionError')
            # except Exception as e:
            #     logger.warning(f'exception occurred, err={e}, url={url}', exc_info=True)
