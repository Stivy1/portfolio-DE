from services.db_query import query_df
from core.treintaLogger import get_logger
from services.secret_manager import access_secret_version
from services.send_mongo import send_products_mongo
from pymongo import MongoClient, UpdateOne, InsertOne
from tqdm import tqdm

logger = get_logger("Products STD - Mongo Uploaded", "DEBUG")

secret_id = "TREINTA-DEV-MONGO"
secret_id_prod = "TREINTA-PROD-MONGO"
version = "latest"
project_id = "692857860374"

MONGO_URI = access_secret_version(project_id=project_id, secret_id=secret_id, version_id=version)
MONGO_URI_PROD = access_secret_version(project_id=project_id, secret_id=secret_id_prod, version_id=version)
db = 'products'
mongo_client = MongoClient(MONGO_URI)
mongo_prod = MongoClient(MONGO_URI_PROD)

db_dev = mongo_client[db]
db_prod = mongo_prod[db]

#Subir scrappers_providers

logger.debug("Scrap Providers")

collection_scrap_pr = 'scrap-providers'
BATCH_LOAD_SIZE = 10000

df_scrap_pr = query_df(query_str="SELECT * FROM `data-production-337318.products_standarization.providers`")

requests = []
for _, row in tqdm(df_scrap_pr.iterrows()):

    body = {
        "provider_id": row['provider_id'],
        "warehouse_name": row["warehouse_name"]
        }

    requests.append(UpdateOne(body,{"$set": body}, upsert=True))
    if len(requests) >= BATCH_LOAD_SIZE:
        print(f"Numero total de registros {len(requests)}")
        send_products_mongo(db_dev[collection_scrap_pr], requests)
        send_products_mongo(db_prod[collection_scrap_pr], requests)
        requests = []

send_products_mongo(db_dev[collection_scrap_pr], requests)
send_products_mongo(db_prod[collection_scrap_pr], requests)

#Subir B2B warehouse
logger.debug("B2B Warehouses")

collection_b2b_ware = 'b2b_warehouses'
BATCH_LOAD_SIZE = 10000

df_b2b_ware = query_df(query_str="SELECT * FROM `treintaco-cz.products_standarization.b2b_warehouses`")
requests = []
for _, row in tqdm(df_b2b_ware.iterrows()):

    body = {
        "warehouse_name": row['warehouse_name'],
        "warehouse_id": row["warehouse_id"]
        }

    requests.append(UpdateOne(body, {"$set": body}, upsert=True))
    if len(requests) >= BATCH_LOAD_SIZE:
        print(f"Numero total de registros {len(requests)}")
        send_products_mongo(db_dev[collection_b2b_ware], requests)
        send_products_mongo(db_prod[collection_b2b_ware], requests)
        requests = []

send_products_mongo(db_dev[collection_b2b_ware], requests)
send_products_mongo(db_prod[collection_b2b_ware], requests)

#Subir Scrappers Products
logger.debug("Scrappers Products")

collection_scrp_prod = 'scrappers_clean'
BATCH_LOAD_SIZE = 10000

df_scrap_prod = query_df(query_str="SELECT * FROM `data-production-337318.products_standarization.mongo_scrappers`")
requests = []
for _, row in tqdm(df_scrap_prod.iterrows()):

    filter = {
        "name": row["name"], 
        "ean": row["ean"]
    }

    update = {
        "$set": 
            {
                "last_sync": row['last_sync'],
                "price": row['price']
            },
        "$setOnInsert":
            {
                "name": row["name"],
                "clean_name": row['clean_name'],
                "provider_id": row['provider_id'],
                "warehouse_name": row['warehouse_name'],
                "ean": row['ean'],
                "sku": row['sku'],
            }
        }

    # body = {
    #     "name": row["name"],
    #     "clean_name": row['clean_name'],
    #     "provider_id": row['provider_id'],
    #     "ean": row['ean'],
    #     "sku": row['sku'],
    #     "last_sync": row['last_sync'],
    #     "price": row['price']
    # }

    requests.append(UpdateOne(filter, update, upsert=True))
    #requests.append(InsertOne(body))
send_products_mongo(db_dev[collection_scrp_prod], requests)
send_products_mongo(db_prod[collection_scrp_prod], requests)

#Subir B2B Products
logger.debug("B2B Products")

collection_b2b_prod = 'b2b_products'
BATCH_LOAD_SIZE = 10000

df_b2b_prod = query_df(query_str="SELECT * FROM `data-production-337318.products_standarization.mongo_B2B`")
requests = []
for _, row in tqdm(df_b2b_prod.iterrows()):

    filter = {
        "product_id": row["product_id"],
        "name": row["name"], 
        "ean": row["ean"]
    }

    update = {
        "$set": 
            {
                "ean": row["ean"],
                "price": row['price'],
                "active": row["active"],
            },
        "$setOnInsert":
            {
                "name": row['name'],
                "clean_name": row["clean_name"],
                "warehouse_name": row["warehouse_name"],
                "warehouse_id": row["warehouse_id"],
                "product_id": row["product_id"],
                "sku": row["sku"],
                "location_id": row["location_id"],
                "category": row["category"],
            }
    }

    # body = {
    #     "name": row["name"],
    #     "clean_name": row['clean_name'],
    #     "warehouse_name": row['warehouse_name'],
    #     "warehouse_id": row['warehouse_id'],
    #     "product_id": row['product_id'],
    #     "ean": row['ean'],
    #     "sku": row['sku'],
    #     "location_id": row['location_id'],
    #     "price": row['price'],
    #     "active": row['active']
    # }

    #requests.append(InsertOne(body))
    requests.append(UpdateOne(filter, update, upsert=True))

send_products_mongo(db_dev[collection_b2b_prod], requests)
send_products_mongo(db_prod[collection_b2b_prod], requests)
