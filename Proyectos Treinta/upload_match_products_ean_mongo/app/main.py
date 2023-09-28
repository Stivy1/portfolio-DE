from google.cloud import bigquery
from pymongo import MongoClient
import dask_bigquery
import os
from datetime import datetime, timedelta
from threading import Thread
from queue import Queue
import time, math
from google.cloud import secretmanager
from pymongo import UpdateOne, InsertOne
from tqdm import tqdm
import json
import numpy as np

def truncate_dev():

    secrets = secretmanager.SecretManagerServiceClient()
    MONGO_URI = secrets.access_secret_version(request={"name": f"projects/692857860374/secrets/TREINTA-DEV-MONGO/versions/latest"}).payload.data.decode("utf-8")
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["products"]
    collection = db["ean_products_match"]

    result = collection.delete_many({})

    print(f"{result.deleted_count} documentos eliminados de la colección {collection.name}.")

def truncate_prod():

    secrets = secretmanager.SecretManagerServiceClient()
    MONGO_URI = secrets.access_secret_version(request={"name": f"projects/692857860374/secrets/TREINTA-PROD-MONGO/versions/latest"}).payload.data.decode("utf-8")
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["products"]
    collection = db["ean_products_match"]

    result = collection.delete_many({})

    print(f"{result.deleted_count} documentos eliminados de la colección {collection.name}.")

def dev():

    truncate_dev()

    secrets = secretmanager.SecretManagerServiceClient()
    MONGO_URI = secrets.access_secret_version(request={"name": f"projects/692857860374/secrets/TREINTA-DEV-MONGO/versions/latest"}).payload.data.decode("utf-8")
    DB = "products"
    COLLECTION = "ean_products_match"
    BATCH_LOAD_SIZE = 1000
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB]
    target_project = "data-production-337318"
    target_dataset = "products_standarization"
    target_table = "retool_ean_products_match"
    # Extraction config
    PROJECT_ID = target_project
    DATASET = target_dataset
    TABLE = target_table
    # Load Dataframe
    ddf = dask_bigquery.read_gbq(
        project_id=PROJECT_ID,
        dataset_id=DATASET,
        table_id=TABLE,
    )

    def upload_mongo(mongoCollection, requests):
        try:
            result = mongoCollection.bulk_write(requests, ordered=False)
            response = result.bulk_api_result
            if response['writeErrors'] != []:
                print(response['writeErrors'])
        except Exception as bwe:
            print(bwe)

    requests = []

    for _, row in tqdm(ddf.iterrows()):

        scrapper_data = row["scrapper_data"].tolist()
        
        body = {
            "id": row["id"],
            "product_id": row["product_id"],
            "sku": row["sku"],
            "ean": row["ean"],
            "name": row["name"],
            "price": row["price"],
            "discount_value": row["discount_value"],
            "price_with_discount": row["price_with_discount"],
            "warehouse_id": row["warehouse_id"],
            "warehouse_name": row["warehouse_name"],
            "b2b_stock": row["b2b_stock"],
            "image_url": row["image_url"],
            "category": row["category"],
            "visible_ffvv": row["visible_ffvv"],
            "priority": row["priority"],
            "category_SOCIO": row["category_SOCIO"],
            "ignore_price": row["ignore_price"],
            "competitors_visible": row["competitors_visible"],
            "scrapper_data": scrapper_data
        }
        requests.append(InsertOne(body))
        if len(requests) >= BATCH_LOAD_SIZE:
            #print(f"Numero total de registros {len(requests)}")
            upload_mongo(db[COLLECTION], requests)
            requests = []


    upload_mongo(db[COLLECTION], requests)

def prod():

    truncate_prod()

    secrets = secretmanager.SecretManagerServiceClient()
    MONGO_URI = secrets.access_secret_version(request={"name": f"projects/692857860374/secrets/TREINTA-PROD-MONGO/versions/latest"}).payload.data.decode("utf-8")
    DB = "products"
    COLLECTION = "ean_products_match"
    BATCH_LOAD_SIZE = 1000
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB]
    target_project = "data-production-337318"
    target_dataset = "products_standarization"
    target_table = "retool_ean_products_match"
    # Extraction config
    PROJECT_ID = target_project
    DATASET = target_dataset
    TABLE = target_table
    # Load Dataframe
    ddf = dask_bigquery.read_gbq(
        project_id=PROJECT_ID,
        dataset_id=DATASET,
        table_id=TABLE,
    )

    def upload_mongo(mongoCollection, requests):
        try:
            result = mongoCollection.bulk_write(requests, ordered=False)
            response = result.bulk_api_result
            if response['writeErrors'] != []:
                print(response['writeErrors'])
        except Exception as bwe:
            print(bwe)

    requests = []

    for _, row in tqdm(ddf.iterrows()):
        
        scrapper_data = row["scrapper_data"].tolist()
        
        body = {
            "id": row["id"],
            "product_id": row["product_id"],
            "sku": row["sku"],
            "ean": row["ean"],
            "name": row["name"],
            "price": row["price"],
            "discount_value": row["discount_value"],
            "price_with_discount": row["price_with_discount"],
            "warehouse_id": row["warehouse_id"],
            "warehouse_name": row["warehouse_name"],
            "b2b_stock": row["b2b_stock"],
            "image_url": row["image_url"],
            "category": row["category"],
            "visible_ffvv": row["visible_ffvv"],
            "priority": row["priority"],
            "category_SOCIO": row["category_SOCIO"],
            "ignore_price": row["ignore_price"],
            "competitors_visible": row["competitors_visible"],
            "scrapper_data": scrapper_data
        }
        requests.append(InsertOne(body))
        if len(requests) >= BATCH_LOAD_SIZE:
            #print(f"Numero total de registros {len(requests)}")
            upload_mongo(db[COLLECTION], requests)
            requests = []


    upload_mongo(db[COLLECTION], requests)    

# truncate()
dev()
prod()
