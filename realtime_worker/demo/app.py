# app.py

from flask import Flask, jsonify
from spider import crawl
from compare import parse


import json
app = Flask(__name__)

testdata = {
    "productCode": "P4967498937327497A",

    "super_category_code": "cats",
    "text": "aaaa",     #搜索框输入文本
    "storeCode": "BRWNPT2GPU",
    "xlat": "250440648",
    "xlng": "551860877",

    #非必要
    "page": 2,
}

# 定义一个路由，当访问 http://127.0.0.1:5000/scrape 时运行爬虫
@app.route('/scrape')
def scrape():
    try:
        # scraping logic here
        result = crawl(testdata["page"],testdata["text"],testdata["storeCode"],testdata["super_category_code"],testdata["xlat"], testdata["xlng"])  # 调用你的爬虫函数
        productName, price = parse(result, testdata["productCode"]) #搜索

        return jsonify({"productName": productName, "dicPrice": price})

    except Exception as e:
        return jsonify({"error": str(e)})



if __name__ == '__main__':
    app.run(debug=True)
