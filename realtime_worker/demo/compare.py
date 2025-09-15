from jsonsearch import JsonSearch
import json


def parse(result,productCode):
    # print("result: ",result)
    # print("productCode:",productCode)
    jsonsearch = JsonSearch(object=result, mode='j')
    resultProductCode = jsonsearch.search_all_value(key='productCode')
    discountedPrice = jsonsearch.search_all_value(key='discountedPrice')
    nameEn = jsonsearch.search_all_value(key='nameEn')

    for i in range(len(resultProductCode)):
        try:
            if (productCode == resultProductCode[i]):

                product_price = discountedPrice[i]

                product_name = nameEn[i]

                product_active = True

                return product_name,product_price,product_active

        except Exception as e:
            print("none products",e)
    product_active = False
    return None,None,product_active



