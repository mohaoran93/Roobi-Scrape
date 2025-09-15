import requests

# API_KEY = "7e558db946b846f8"
API_KEY = "hcr12m10s49l9r8j"

def nownow_list_stores(latitude, longitude):
    url = "https://now-now.xbyteapi.com/now_now_uae_api"
    params = {"api_key":API_KEY,"latitude":latitude,"longitude":longitude,"endpoint":"nownow_uae_app_store_listing"}
    response = requests.get(url,params=params)
    print(response.json())
    # v3 https://now-now.xbyteapi.com/now_now_uae_api?api_key=hcr12m10s49l9r8j&latitude=25.1933243&longitude=55.2784980&endpoint=nownow_uae_app_store_listing
    # {'request_log': {'requests_url': 'https://now-now.xbyteapi.com/now_now_uae_api?apikey=7e558db946b846f8&endpoint=store_api&latitude=25.372396787184577&longitude=55.39592658799068', 'request_time': '2024-04-09 12:29:51.043624+00:00', 'request_process_time': '16.39563822746277'}, 'results': {'status': 200, 'data': {'stores': None}, 'message': 'store not found'}}
    return response.json()

if __name__ == "__main__":
    res = nownow_list_stores(25.0976119,55.163109)
    print(res)