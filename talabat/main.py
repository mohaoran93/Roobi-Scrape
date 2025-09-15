import time
import sys
import os
import json
import argparse
import logging
import h3
from datetime import datetime
from elasticsearch import helpers

from talabat.xbytes.call_xbytes import (
    get_stores,
    get_store_categories,
    get_products,
    get_mart_stores,
    get_mart_products
)
from functions import (
    get_esinstance,
    index_name_store,
    index_name_product,
    CACHED_TIME_THRESHOLD,
    create_index_if_not_exists_with_mapping,
    clear_index,
    delete_index,
    talabat_store_mapping,
    talabat_product_mapping
)

# from tools import send_telegram_message

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("Talabat-Scrapy")

es = get_esinstance()

def run_talabat(latitude, longitude, resolution=10, clear_existing=False, recreate_index=False):
    """
    Run Talabat scraper
    
    Args:
        latitude: Location latitude
        longitude: Location longitude  
        resolution: H3 resolution
        clear_existing: If True, clear existing data before scraping
        recreate_index: If True, delete and recreate the indices
    """
    t1 = time.time()
    cached = 0
    total_products = 0
    h3_cell = h3.geo_to_h3(latitude, longitude, resolution)
    geo = {"lat": latitude, "lon": longitude}

    # Index management
    if recreate_index:
        logger.info("Recreating indices…")
        delete_index(es, index_name_store)
        delete_index(es, index_name_product)
        create_index_if_not_exists_with_mapping(es, index_name_store,   talabat_store_mapping)
        create_index_if_not_exists_with_mapping(es, index_name_product, talabat_product_mapping)
    elif clear_existing:
        logger.info("Clearing existing data…")
        clear_index(es, index_name_store)
        clear_index(es, index_name_product)
    else:
        create_index_if_not_exists_with_mapping(es, index_name_store,   talabat_store_mapping)
        create_index_if_not_exists_with_mapping(es, index_name_product, talabat_product_mapping)

    # Step 1: fetch grocery stores
    try:
        stores = get_stores(latitude, longitude)
        logger.info(f"Found {len(stores)} Talabat stores")
        if not stores:
            logger.error("No stores, aborting")
            return
        os.makedirs("temp", exist_ok=True)
        with open(f"temp/talabat_stores_{h3_cell}.json", "w") as f:
            json.dump(stores, f, indent=4)
    except Exception as e:
        logger.error(f"Error fetching stores: {e}")
        return

    # Step 2: process each store
    for store in stores:
        now_iso = datetime.utcnow().isoformat()
        store_id = store.get("store_id")
        bid      = store.get("bid")
        if not store_id or not bid:
            logger.warning("Store missing ID or bid, skipping")
            continue

        # check cache
        last_time = datetime.fromisoformat("1990-01-01T00:00:00")
        if es.indices.exists(index=index_name_store):
            try:
                q = {
                    "query": {"term": {"bid": bid}},
                    "sort": [{"last_scraped_time": {"order": "desc"}}]
                }
                hits = es.search(index=index_name_store, **q)["hits"]["hits"]
                if hits:
                    last_time = datetime.fromisoformat(hits[0]["_source"]["last_scraped_time"])
            except:
                pass

        # if stale, fetch categories & products
        if (datetime.utcnow() - last_time).total_seconds() > int(CACHED_TIME_THRESHOLD):
            try:
                cats = get_store_categories(latitude, longitude, store_id)
                for cat in cats:
                    sub_id = cat.get("subcategory_id")
                    # pagination
                    page = 0
                    while True:
                        prods, pag = get_products(store_id, sub_id, page_no=page)
                        if not prods:
                            break
                        actions = [{
                            "_index": index_name_product,
                            "_id": f"{store_id}_{bid}_{p['product_id']}_test",
                            "_source": {
                                **p,
                                "store_id":         store_id,
                                "bid":              bid,
                                "h3_cell":          h3_cell,
                                "geo":              geo,
                                "last_scraped_time": now_iso,
                                "platform":         "talabat",
                                "test_run":         True
                            }
                        } for p in prods]
                        helpers.bulk(es, actions, raise_on_error=True)
                        total_products += len(prods)
                        if page >= pag.get("total_pages", 1) - 1:
                            break
                        page += 1

                # index store
                store_doc = {
                    **store,
                    "h3_cell":           h3_cell,
                    "geo":               geo,
                    "last_scraped_time": now_iso,
                    "test_run":          True
                }
                es.index(
                    index=index_name_store,
                    id=f"{store_id}_{h3_cell}_test",
                    document=store_doc
                )

            except Exception as e:
                logger.error(f"Error processing store {store_id}: {e}")
        else:
            cached += 1
            logger.info(f"Store {store_id} skipped (cached)")
            doc = {
                **store,
                "h3_cell":           h3_cell,
                "geo":               geo,
                "last_scraped_time": now_iso,
                "test_run":          True
            }
            es.index(
                index=index_name_store,
                id=f"{store_id}_{h3_cell}_test",
                document=doc
            )

    duration = int(time.time() - t1)
    logger.info(f"Done in {duration}s — stores: {len(stores)}, cached: {cached}, products: {total_products}")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description='Talabat Scraper')
    p.add_argument('geoloc', help='Lat,Lng')
    p.add_argument('--clear',   action='store_true', help='Clear existing data')
    p.add_argument('--recreate',action='store_true', help='Recreate indices')
    args = p.parse_args()

    lat, lon = map(float, args.geoloc.split(","))
    logger.info(f"Starting Talabat at {lat},{lon}")
    run_talabat(lat, lon, clear_existing=args.clear, recreate_index=args.recreate)
