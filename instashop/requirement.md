# Scraping Service for instashop

# API 1 get all stores
Input: lat,lng example lat,lng
Output: List of all stores for that location. Return field name exactly as the example
Example
```sh
[{
        "name": "Armaan Minimarket",
        "image": "https://cdn.instashop.ae/5a4c2791353e0b59307b5090c37f752d_armanminimarket500x500_-1-.png",
        "store_id": "ztefdj1du5",
        "delivery_fee": 5.75,
        "delivery_eta": 15,
        "min_basket": 30,
        "operationalStatus":, # Store is open or not.
        "companyName": "Armaan Mini Market",
        "own_delivery": true,
        "contactInfo": {
            "mobile": "00971588346625",
            "phone": "00971588933762",
            "address": "Al Raha",
            "email": "contact@instashop.ae",
            "localizedAddress": {
                "ar": "\u0627\u0644\u0631\u0627\u062d\u0629"
            }
        }
},
...
]
```

# API 2 get all products for a given store_id
input: store_id,lat,lng
Output: all products from this store.
Example: 
```sh
[{"store_id": "lPsPbaIHjK", "location_id": 1, "product_id": "5hjj7ZqaIZ", "product_name": "Durex Extra Safe Condoms", "product_category": "Sensual Care", "product_subcategory": "Condoms", "product_brand": "Durex", "product_price": 64.95, "product_link": "https://cdn.instashop.ae/w-5bbb9798-7928-4969-b137-e3e7d9b93293382a981c2a8f140c577fd83f7df1ccb1_IS_20617.JPG", "product_image": "https://cdn.instashop.ae/64f5039d-5400-42a2-b291-307f4e92e40a4f69798b68517444c900ffcf6243e6fa_5010232964501.jpg", "category_id": "VDi7N7Z4IN", "first_category_id": "VDi7N7Z4IN", "second_category_id": "3Vr8vH326v", "packagingString": "12 per pack", "pricePerUnit": "", "product_price_all": {"withMargin": 64.95, "round": 64.95, "retail": 64.95}, "excludedFromMinimumOrder": false}]
```

# API 3 get product realtime price
input: store_id,product_id
output:
[
    'product_id': product_id,
    'store_id': key,
    'product_name': product['title'],
    'product_price': clients[key]['price']['retail'],
    'product_active': clients[key]['active']
]
When you scrpe this product, try to include more products you have to scrape, I will search in the list by myself.



# Notes
1. You can return additional field, but the required field need to be exact name as the example.
2. API 1 need to exclude 'Restaurants'
3. I notice one strange thing when I scrape, two close geo points (as close as 200 meters) can return very different list of stores. Instashop may return fake store list.
4. If API 2 can not be achieved due to one store may have too many products, we can discuss a solution.

