from google.cloud import bigquery
from pymongo import MongoClient
import pandas_gbq
from google.cloud import secretmanager
from pymongo import UpdateOne, InsertOne
from tqdm import tqdm
import pandas as pd
import re

secrets = secretmanager.SecretManagerServiceClient()
MONGO_URI = secrets.access_secret_version(request={"name": f"projects/692857860374/secrets/TREINTA-PROD-MONGO/versions/latest"}).payload.data.decode("utf-8")

mongo_client = MongoClient(MONGO_URI)

# Get data products of merchants that has B2B
def get_bk_products():

    # Change the query with the table and information 
    query = ("SELECT id, name FROM `treintaco-lz.treintagpn_b2b.warehouses`")

    df = pandas_gbq.read_gbq(query)
    print(df)
    return df

def send_products_mongo(mongoCollection, requests):

    try:
        #print(_min,_max,len(requests[_min:_max]))
        result = mongoCollection.bulk_write(requests, ordered=False)
        response = result.bulk_api_result
        #print(response)
        if response['writeErrors'] != []:
            print(response['writeErrors'])
    except Exception as bwe:
        print(bwe)

# Metadata to reference DB
db = 'products'
db = mongo_client[db]

# Change the collection name
collection = 'b2b_warehouses'
BATCH_LOAD_SIZE = 10000
final_df = get_bk_products()
requests = []
for _, row in tqdm(final_df.iterrows()):

    body = {
        "pwarehouse_id": row["id"],
        "warehouse_name": row['name']
    }

    requests.append(InsertOne(body))
    # requests.append(UpdateOne(filter, update, upsert=True))
    if len(requests) >= BATCH_LOAD_SIZE:
        print(f"Processing {len(requests)} rows...")
        send_products_mongo(db[collection], requests)
        requests = []
print(f"Processed {len(final_df)} rows.")

send_products_mongo(db[collection], requests)
