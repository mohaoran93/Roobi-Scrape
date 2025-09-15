# （1） 获取网页的源码
# （2） 解析   解析的服务器响应的文件  etree.HTML
# (3)  打印

import urllib.request
import requests
import json
from jsonsearch import JsonSearch
import json



def generate_url(searchparam,store_code,super_category_code,page=None):
    #一个店铺返回1页10个商品
    if( page==None ):
        url = 'https://mp-shop-api-catalog.fd.noon.com/v2/content/search?type=product&page=1&limit=10&q={}&f[store_code]={}&f[super_category_code]={}'.format(
            searchparam, store_code, super_category_code)
    else:
        url = 'https://mp-shop-api-catalog.fd.noon.com/v2/content/search?type=product&page={}&limit=10&q={}&f[store_code]={}&f[super_category_code]={}'.format(
        page,searchparam,store_code,super_category_code)
    return url




def headers(xlat,xlng):
    headers = {
        'x-lat': xlat,
        'x-lng': xlng,
        'User-Agent': 'noonNow / 2 CFNetwork / 1335.0.3 Darwin / 21.6.0',
        # seems no need to have cookie, I am removing it to avoid Nownow account get banned
        # 'Cookie': 'ak_bmsc=8CDF1FD2F8E63B03B20FBCB270B38457~000000000000000000000000000000~YAAQLvV0aNoOTCGMAQAAR/cySBZ6h8we7MHpaGUnAx8x6gN2wIsb8cXmxIjQop4AN2K5ndBAWK5gER/LbfNNoQwGtj7oz5S93cw/NHJYIAxMf0UwON26Y7MXter14YyAPxZ4p5wtBF0wEMYLASwrR1uQ974WykhJxWxBCFhYDiM/jA+t/kpVRMjjNzwVptDVpIvUqmnGdvTaQqIrnZ3JkIeYoNiHAJL17Uz8djdOtMyb6LawOpWuhWZbZf1yIyFtwSKuEz4sUgMMDyVWM2/G5bMA7HQfMfBjox2tztIMoyilDXHJpyP5g6ZjAB+wMmGltMteQCLOS9wWcQIwk3QYrV7Zl1tfrwYwUmMi8OouBeI8aqrr7iSPBw==; nownow_customer_cookie=c8f0d310-783d-4621-85de-bc1ec3ba969f',
    }
    # headers = {
    #         'x-lng': str(xlat), # '551475630',#
    #         'x-lat': str(xlng), # '250867220', # 
    #         'x-address-key': 'NNA38324636375971022A:1', # this is marina gate
    #         "x-version": "2.3",
    #         "x-device-id": 'iPhone9,1',
    #         'x-tenant': 'basket',
    #         'Host': 'mp-shop-api-catalog.fd.noon.com',
    #         'x-locale': 'en-AE',
    #         'x-language': 'en',
    #         'Accept': 'application/json, text/plain, */*',
    #         'Connection': 'keep-alive',
    #         'User-Agent': 'noonNow / 2 CFNetwork / 1335.0.3 Darwin / 21.6.0',
    #         # 'Cookie': 'ak_bmsc=69DCE0D3368A8E44B98163EE5CCD66BB~000000000000000000000000000000~YAAQFXs1F0VA7luMAQAA8OvLpRay+Kcrk0ifQpIysi38qPtFjKr3MRXCnpKUNvK7a9TQ2/8pgsOpH17XK8mFORWoVTaLCl97pfYzCDFSSi94i0xTRUK84SjcXoI7HpsIZjon/1XSfkCXOR/MVIIvAyf3MtIF3id3LrSlU6szZCTl6P9J6mgPkMc/Mzp/jLQKlqmO5BWOrMqO0F3hO/G6d1jykEDKrHcmsxe2Qw+aWYstPZFLP242mCEyWltOU0jTbwR+L2ire/nBnWzxa+o9z4w+mroyiaWPawG+w0FyQOkDqSfIdTWt9xdo9hrBNwKalSppJf3PBDA1aRjS6atLD8K3fHUxFOij4TVkagCdOt0mpAwdciP4Cw==; nownow_customer_cookie=c8f0d310-783d-4621-85de-bc1ec3ba969f',
    # }

    return headers

# https://mp-shop-api-catalog.fd.noon.com/v2/content/search?type=store&page=1&limit=20
# https://mp-shop-api-catalog.fd.noon.com/v2/content/search?type=super_category&page=1&f[store_code]=FMRTETU0GO
# https://mp-shop-api-catalog.fd.noon.com/v1/store/BRWNPT2GPU/cats?page=1&limit=30&category_code=all
# headers = {
#
#     # 'x-locale': 'en-AE',
#     # 'Connection': 'keep-alive',
#     # 'x-country-code': 'UAE',
#     # 'x-version': '2.3',
#     'x-address-key': 'NNA38324636375971022A:1',
#     # 'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
#     # 'Accept-Encoding': 'gzip, deflate, br',
#     'x-lat': '250440648',
#     # 'x-device-id': 'iPhone9,1',
#     # 'x-platform': 'ios',
#     'x-lng': '551860877',
#     # 'x-build': '2',
#     # 'Cookie': 'bm_sv=D067BA7F17308ACC7C3E94A8FE96D882~YAAQdfV0aJU9QN6LAQAAdRc2HxWDV9MnrbVEYQIVEBN2J6xWaSe/CgX2nuaFUWl2fVGyfFKpeQCd6LsHf/jq83C9aXcMeOuRNg3SYGDQvYNID1CIMOwJ9f1zQ6fKokoOsJtDSh4YSBCw6Ix9fzZz01ZTOPfnZD/J6xrlVjWh8p1rORwvS8mpokOqspzITPyeYQP4dPRDWeCvc5dlpO0sV6sxjPWT4i0nRJqDjU0h9NPEWdSSYHrVvG0u3BdbmA==~1; ak_bmsc=84F0A3115975C8BE2F8EFABB732654ED~000000000000000000000000000000~YAAQr1cyuEg5ZNOLAQAA1m00HxW0H5ch8mA/0iuc9psMldTDSCBGX22bSCO9su8DPkjM58i1jy9Y757gmCboVvVQw+LHJtvVSbWc8cIo9RBuHRX3FGNVQlfwkElb234NuGEP42lTcweny3Ufcx/dJ9ilk5hfaYSbwxNlgNNr0b8UpYzb+qvYNjKtftPqyjfMuaZO+cU/Q99VDC9Pu5PGChFlUzkb7SM1UYgR5uOJkxhenkXFQTQROugN2chGnvP+Z3XUy10xPqbsLReEgCZt/PfX+kv9Ackr8fulxAyQPnADKG6864KD+5xHIJuol+pOhNvmYhMzl6zBCRdDkcqIr9spVABCso2i43xQBSx2rfc8uQzmQtYcf7JoS3wqJnZuRJl/cTcNNlGY; nownow_customer_cookie=c8f0d310-783d-4621-85de-bc1ec3ba969f',
#     # 'x-tenant': 'basket',
#     # 'Date': 'Thu, 30 Nov 2023 07:51:50 GMT',
#     # 'Content-Type': 'application/json',
#     # 'Connection': 'keep-alive',
#     # 'x-authproxy': 'l10',
#     # 'x-permitted-cross-domain-policies': 'none',
#     # 'x-process-time': '0.45013427734375',
#     # 'x-envoy-upstream-service-time': '467',
#     # 'referrer-policy': 'strict-origin-when-cross-origin',
#     # 'x-content-type-options': 'nosniff',
#     # 'x-frame-options': 'DENY',
#     # 'x-xss-protection': '1; mode=block',
#     # 'via': '1.1 google',
#     # 'CF-Cache-Status': 'DYNAMIC',
#     # 'Server': 'cloudflare',
#     # 'CF-RAY': '82e181ea6bd374de-MAD',
#     # 'Content-Length': '40562',
#
#
#
#     'GET https':'//mp-shop-api-catalog.fd.noon.com/v2/content/search?type=store&page=1&limit=20 HTTP/1.1',
#     'Host': 'mp-shop-api-catalog.fd.noon.com',
#     'x-locale': 'en-AE',
#     'x-language': 'en',
#     'Accept': 'application/json, text/plain, */*',
#     'Connection': 'keep-alive',
#     'User-Agent': 'noonNow / 2 CFNetwork / 1335.0.3 Darwin / 21.6.0',
#     'Cookie': 'bm_sv=D067BA7F17308ACC7C3E94A8FE96D882~YAAQdfV0aJU9QN6LAQAAdRc2HxWDV9MnrbVEYQIVEBN2J6xWaSe/CgX2nuaFUWl2fVGyfFKpeQCd6LsHf/jq83C9aXcMeOuRNg3SYGDQvYNID1CIMOwJ9f1zQ6fKokoOsJtDSh4YSBCw6Ix9fzZz01ZTOPfnZD/J6xrlVjWh8p1rORwvS8mpokOqspzITPyeYQP4dPRDWeCvc5dlpO0sV6sxjPWT4i0nRJqDjU0h9NPEWdSSYHrVvG0u3BdbmA==~1; ak_bmsc=84F0A3115975C8BE2F8EFABB732654ED~000000000000000000000000000000~YAAQr1cyuEg5ZNOLAQAA1m00HxW0H5ch8mA/0iuc9psMldTDSCBGX22bSCO9su8DPkjM58i1jy9Y757gmCboVvVQw+LHJtvVSbWc8cIo9RBuHRX3FGNVQlfwkElb234NuGEP42lTcweny3Ufcx/dJ9ilk5hfaYSbwxNlgNNr0b8UpYzb+qvYNjKtftPqyjfMuaZO+cU/Q99VDC9Pu5PGChFlUzkb7SM1UYgR5uOJkxhenkXFQTQROugN2chGnvP+Z3XUy10xPqbsLReEgCZt/PfX+kv9Ackr8fulxAyQPnADKG6864KD+5xHIJuol+pOhNvmYhMzl6zBCRdDkcqIr9spVABCso2i43xQBSx2rfc8uQzmQtYcf7JoS3wqJnZuRJl/cTcNNlGY; nownow_customer_cookie=c8f0d310-783d-4621-85de-bc1ec3ba969f',
#
# }

# 请求对象的定制

def request(url,headers):

    response = requests.get(url=url, headers=headers, verify=False)

    content = response.text

    content = json.loads(content)

    return content



def crawl(page,searchparam,store_code,super_category_code, xlat, xlng):

    base_url = generate_url(page=page,searchparam=searchparam,store_code=store_code,super_category_code=super_category_code)

    header = headers(xlat=xlat, xlng=xlng)

    result = request(url=base_url, headers=header)

    return result


SMARTPROXY_USER = 'brd-customer-hl_252e675d-zone-data_center'  # Your Smartproxy Username
SMARTPROXY_PASSWORD = 'pe6xao2s9z9k'  # Your Smartproxy Password
SMARTPROXY_ENDPOINT = 'brd.superproxy.io'  # Endpoint you'd like to use
SMARTPROXY_PORT = '22225'  # Port provided by Smartproxy

proxies = {
    "http": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASSWORD}@{SMARTPROXY_ENDPOINT}:{SMARTPROXY_PORT}",
    "https": f"https://{SMARTPROXY_USER}:{SMARTPROXY_PASSWORD}@{SMARTPROXY_ENDPOINT}:{SMARTPROXY_PORT}"
}
# proxies = None
def crawl_v2(searchparam,store_code,super_category_code, xlat, xlng):
    # update 1, text must be full text, therefore we use page=1 only
    # update 2, pass parameters in para to avoid special charactor encoding issue
    url = 'https://mp-shop-api-catalog.fd.noon.com/v2/content/search'
    header = headers(xlat=xlat, xlng=xlng)
    if super_category_code == None or super_category_code == "":
        super_category_code = "all"
    params = {"type":"product","page":1,"limit":10,"q":searchparam,"f[store_code]":store_code,"f[super_category_code]":super_category_code}
    response = requests.get(url=url, params=params,headers=header, verify=False,proxies=proxies)
    content = response.text
    content = json.loads(content)
    return content






# print(content["results"][5]["results"][0]["storeCode"])
# for i in range(10):
#     goodsPhoto = "https://f.nooncdn.com/nownow/"+content["data"]["products"][i]["images"][0]
#     superCategoryCode= content["data"]["products"][0]["superCategoryCode"]
#     goodsPrice = content["data"]["products"][i]["price"]
#
#
#     url = goodsPhoto
#     filename = './goods/' + str(superCategoryCode) + str(goodsPrice) +'   AED.jpg'



# request = urllib.request.Request(url=url, headers=headers)
# response = urllib.request.urlopen(request)
# content = response.text

