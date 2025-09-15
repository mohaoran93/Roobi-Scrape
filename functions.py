from elasticsearch import Elasticsearch, NotFoundError
from dotenv import dotenv_values
import logging

logger = logging.getLogger(__name__)

config = dotenv_values(".env")
print(f"config {config}")
ES_HOST = config["ES_HOST"]
ELASTIC_USER = config["ELASTIC_USER"]
ELASTIC_PASSWORD = config["ELASTIC_PASSWORD"]
CACHED_TIME_THRESHOLD = config["CACHED_TIME_THRESHOLD"]
index_name_store = config["instashop_index_name_store"]
index_name_product = config["instashop_index_name_product"]
careem_quick_index_name_product = config["careem_quick_index_name_product"]
carrefour_index_name_product = config["carrefour_index_name_product"]
instashop_index_name_store = config["instashop_index_name_store"]
instashop_index_name_product = config["instashop_index_name_product"]
kibsons_index_name_product = config["kibsons_index_name_product"]
index_name_product_noon = config["index_name_product_noon"]
nownow_index_name_store = config["nownow_index_name_store"]
nownow_index_name_product = config["nownow_index_name_product"]
talabat_index_name_store = config["talabat_index_name_store"]
talabat_index_name_product = config["talabat_index_name_product"]
CACHED_TIME_THRESHOLD="3600"

def get_esinstance():
    es = Elasticsearch([ES_HOST],
                      basic_auth=(ELASTIC_USER,ELASTIC_PASSWORD),
                      verify_certs=False)
    return es

def create_index_if_not_exists(es, index_name):
    """Create index if it doesn't exist"""
    try:
        if not es.indices.exists(index=index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "product_id": {"type": "keyword"},
                        "product_name": {"type": "text", "analyzer": "standard"},
                        "product_brand": {"type": "keyword"},
                        "product_price": {"type": "float"},
                        "product_image": {"type": "keyword"},
                        "store_id": {"type": "keyword"},
                        "store_name": {"type": "keyword"},
                        "delivery_fee": {"type": "float"},
                        "min_basket": {"type": "float"},
                        "product_category": {"type": "keyword"},
                        "product_subcategory": {"type": "keyword"},
                        "packagingString": {"type": "text"},
                        "last_scraped_time": {"type": "date"},
                        "subcategory_id": {"type": "keyword"},
                        "subcategory_name": {"type": "keyword"},
                        "category_id": {"type": "keyword"},
                        "category_name": {"type": "keyword"},
                        "zone_id": {"type": "keyword"},
                        "h3_cell": {"type": "keyword"},
                        "test_run": {"type": "boolean"}
                    }
                }
            }
            
            es.indices.create(index=index_name, body=mapping)
            logger.info(f"Created index: {index_name}")
        else:
            logger.info(f"Index {index_name} already exists")
    except Exception as e:
        logger.error(f"Error creating index {index_name}: {e}")
        raise

def clear_index(es, index_name):
    """Clear all documents from index (keeps structure)"""
    try:
        if es.indices.exists(index=index_name):
            result = es.delete_by_query(index=index_name, body={"query": {"match_all": {}}})
            logger.info(f"Cleared index {index_name}: deleted {result.get('deleted', 0)} documents")
        else:
            logger.warning(f"Index {index_name} does not exist")
    except Exception as e:
        logger.error(f"Error clearing index {index_name}: {e}")
        raise

def delete_index(es, index_name):
    """Delete the entire index (structure and data)"""
    try:
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            logger.info(f"Deleted index: {index_name}")
        else:
            logger.warning(f"Index {index_name} does not exist")
    except Exception as e:
        logger.error(f"Error deleting index {index_name}: {e}")
        raise

def create_index_if_not_exists_with_mapping(es, index_name, mapping):
    """Create index if it doesn't exist"""
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, **mapping)
            logger.info(f"Created index: {index_name}")
        else:
            logger.info(f"Index {index_name} already exists")
    except Exception as e:
        logger.error(f"Error creating index {index_name}: {e}")
        raise

instashop_store_mapping = {
    "mappings": {
        "properties": {
            "store_id": {"type": "keyword"},
            "store_name": {"type": "text"},
            "name": {"type": "text"},
            "areaName": {"type": "text"},
            "h3_cell": {"type": "keyword"},
            "geo": {"type": "geo_point"},
            "last_scraped_time": {"type": "date"},
            "test_run": {"type": "boolean"},
            "platform": {"type": "keyword"}
        }
    }
}

instashop_product_mapping = {
    "mappings": {
        "properties": {
            "store_id": {"type": "keyword"},
            "product_id": {"type": "keyword"},
            "product_name": {"type": "text", "analyzer": "standard"},
            "product_brand": {"type": "keyword"},
            "product_price": {"type": "float"},
            "product_image": {"type": "keyword"},
            "packagingString": {"type": "text"},
            "delivery_fee": {"type": "float"},
            "min_basket": {"type": "float"},
            "product_category": {"type": "keyword"},
            "product_subcategory": {"type": "keyword"},
            "category_id": {"type": "keyword"},
            "h3_cell": {"type": "keyword"},
            "last_scraped_time": {"type": "date"},
            "test_run": {"type": "boolean"},
            "platform": {"type": "keyword"}
        }
    }
}



nownow_store_mapping = {
    "mappings": {
        "properties": {
            "Store_Code": {"type": "keyword"},
            "Merchant_Code": {"type": "keyword"},
            "Store_Name": {"type": "text"},
            "Store_Type": {"type": "keyword"},
            "Area_Name": {"type": "text"},
            "Is_new": {"type": "boolean"},
            "Min_order_value": {"type": "float"},
            "Store_hour_details": {"type": "text"},
            "Estimated_Delivery_Time": {"type": "integer"},
            "Road_Duration": {"type": "float"},
            "Road_Distance": {"type": "float"},
            "Distance_Unit": {"type": "keyword"},
            "DeliveryType_Code": {"type": "keyword"},
            "Total_store_count": {"type": "integer"},
            "Store_image": {"type": "keyword"},
            "h3_cell": {"type": "keyword"},
            "geo": {"type": "geo_point"},
            "last_scraped_time": {"type": "date"},
            "test_run": {"type": "boolean"}
        }
    }
}

nownow_product_mapping = {
    "mappings": {
        "properties": {
            "Product_Code": {"type": "keyword"},
            "Product_Name": {"type": "text"},
            "Product_Price": {"type": "float"},
            "Product_Image": {"type": "keyword"},
            "Category_code": {"type": "keyword"},
            "Category_name": {"type": "keyword"},
            "Store_Code": {"type": "keyword"},
            "h3_cell": {"type": "keyword"},
            "platform": {"type": "keyword"},
            "last_scraped_time": {"type": "date"},
            "test_run": {"type": "boolean"}
        }
    }
}


talabat_store_mapping = {
    "mappings": {
        "properties": {
            "store_id": {"type": "keyword"},
            "bid": {"type": "keyword"},
            "store_name": {"type": "text"},
            "name": {"type": "text"},
            "areaName": {"type": "text"},
            "delivery_time": {"type": "text"},
            "h3_cell": {"type": "keyword"},
            "geo": {"type": "geo_point"},
            "lat": {"type": "float"},
            "lng": {"type": "float"},
            "location_id": {"type": "keyword"},
            "last_scraped_time": {"type": "date"},
            "test_run": {"type": "boolean"}
        }
    }
}

talabat_product_mapping = {
    "mappings": {
        "properties": {
            "store_id": {"type": "keyword"},
            "vendor_id": {"type": "keyword"},
            "product_id_orig": {"type": "keyword"},
            "product_name": {"type": "text", "analyzer": "standard"},
            "product_brand": {"type": "keyword"},
            "product_original_price": {"type": "float"},
            "product_sale_price": {"type": "float"},
            "product_image": {"type": "keyword"},
            "category": {"type": "keyword"},
            "sub_category": {"type": "keyword"},
            "min_order": {"type": "float"},
            "delivery_fee": {"type": "float"},
            "shop_type": {"type": "integer"},
            "h3_cell": {"type": "keyword"},
            "platform": {"type": "keyword"},
            "last_scraped_time": {"type": "date"},
            "test_run": {"type": "boolean"}
        }
    }
}