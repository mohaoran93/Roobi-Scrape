from elasticsearch import Elasticsearch

es = Elasticsearch(["http://65.2.65.70:9200"],
                  basic_auth=("elastic", "r8ty25MFPQrnhkf"),
                  verify_certs=False)

# List of indexes to delete
indexes_to_delete = [
    "careem_quick_test_product_v5",
    "carrefour_test_product_v5", 
    "instashop_test_store_v5",
    "instashop_test_product_v5",
    "kibsons_test_product_v5",
    "noon_test_product_v5",
    "nownow_test_store_v5", 
    "nownow_test_product_v5",
    "talabat_test_store_v5",
    "talabat_test_product_v5"
]

for index in indexes_to_delete:
    try:
        if es.indices.exists(index=index):
            es.indices.delete(index=index)
            print(f"Deleted: {index}")
        else:
            print(f"Not found: {index}")
    except Exception as e:
        print(f"Error deleting {index}: {e}")