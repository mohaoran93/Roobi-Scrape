# app.py

from flask import Flask, jsonify
from spider import crawl,crawl_v2
from compare import parse


import json
app = Flask(__name__)

testdata = {
    "productCode": "P4967498937327497A",

    "super_category_code": "all",# "cats",
    "text": "Royal Canin Feline Care Nutrition Intense Beauty Jelly Cat Wet Food",     #搜索框输入文本
    "storeCode": "BRWNPT2GPU",
    "xlat": "250440648",
    "xlng": "551860877",

    #非必要
    "page": 2,
}

# testdata = {'platform': 'nownow', 'productCode': 'P8709393096992512A', 'super_category_code': 'all', 'text': 'a',
#              'product_price': None, 'product_active': False, 'storeCode': 'GRBDLFPTDY',"xlat":"250440648","xlng": "551860877"} # stationeryhouseholdelectronics Psi Single Line Exercise Book


if __name__ == '__main__':
    # scraping logic here
    # result = crawl(testdata["page"],testdata["text"],testdata["storeCode"],testdata["super_category_code"],testdata["xlat"], testdata["xlng"])  # 调用你的爬虫函数
    result = crawl_v2(testdata["text"],testdata["storeCode"],testdata["super_category_code"],testdata["xlat"], testdata["xlng"])
    print(f"result {result}")
    product_name,product_price,product_active = parse(result, testdata["productCode"]) #搜索
    print(f"productName {product_name} price {product_price} product_active {product_active}")