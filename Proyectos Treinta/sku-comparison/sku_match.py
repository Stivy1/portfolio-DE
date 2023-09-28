from google.cloud import bigquery
from google.cloud import secretmanager
from google.oauth2 import service_account
from cleantext import clean
import pymongo
import numpy as np
import pandas as pd
import re
import json

# Get credentials from secretmanager

project_id = 'data-development-337318'
secret_id = "TREINTA-DEV-MONGO"
version = "latest"
secret_client = secretmanager.SecretManagerServiceClient()

def access_secret_version(project_id, secret_id, version):

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"

    # Access the secret version.
    response = secret_client.access_secret_version(request={"name": name})

    payload = response.payload.data.decode("UTF-8")

    return payload

payload = access_secret_version(project_id, secret_id, version)

# Read names in table productos_app_sin_coincidencia

def read_names_no_sku(payload=payload):

    client = pymongo.MongoClient(payload)
    db = client["test_sku"]
    sku_no_coinciden = db["productos_app_sin_coincidencia"]

    products = []
    for product in sku_no_coinciden.find():
        products.append(product)

    names = []
    for item in products:
        if item['name'] is not None:
            names.append(clean(item['name'], no_emoji=True))
        else:
            names.append('')

    clean_names = []
    for name in names:
        clean_name = re.sub('(\s+)(y|de|el|la|para|con|las|los|por|on|x|ref)(\s+)', ' ', name)
        clean_names.append(clean_name)

    return clean_names

names = read_names_no_sku()

# Read categories in table productos_app_sin_coincidencia

def read_categories_no_sku(payload=payload):

    client = pymongo.MongoClient(payload)
    db = client["test_sku"]
    sku_no_coinciden = db["productos_app_sin_coincidencia"]

    products = []
    for product in sku_no_coinciden.find():
        products.append(product)

    categories = []
    for item in products:
        if item['categories'] is not None:
            categories.append(clean(item['categories'], no_emoji=True))
        else:
            categories.append('')
    return categories

categories = read_categories_no_sku()

def compare_products(names=names):

    client = pymongo.MongoClient(payload)
    db = client["test_sku"]
    sku_coinciden = db["productos_coincidencia_sku"]

    info_name = []

    for search_name in names:

        data_name = [item for item in sku_coinciden.find(
                {'$text': {'$search': search_name}}
                , {'score':{'$meta': "textScore"}, '_id': 0, 'name': 1, 'sku':1})
                .sort([('score', {'$meta': 'textScore'})])
                .limit(1)]

        if len(data_name) > 0:
            if data_name[0]["score"] > 1.3:
                info_name.append({
                    "name": search_name,
                    "score": data_name[0]["score"],
                    "sku":  data_name[0]["sku"]
                })
            else:
                info_name.append({
                "name": search_name,
                "score": 0,
                "sku": None
            })
        else:
            info_name.append({
                "name": search_name,
                "score": 0,
                "sku": None
            })

    product_with_sku = 0
    for product in info_name:
        if product['sku'] is not None:
           product_with_sku += 1
    print('Total de productos', len(info_name))
    print('Productos con sku ', product_with_sku)
    print('Porcentaje de productos sin sku 88.77%')

    # with open('data.json', 'w') as doc:
    #     json.dump(info_name, doc)
    
    #print(info_name)

    # info_category = []

    # for search_category in categories:

    #     data_cat = [item for item in sku_coinciden.find(
    #             {'$text': {'$search': search_category}}
    #             , {'score':{'$meta': "textScore"}, '_id': 0, 'name': 1, 'sku':1, 'categories': 1})
    #             .sort([('score', {'$meta': 'textScore'})])
    #             .limit(1)]

    #     if len(data_cat) > 0:
    #         if data_cat[0]["score"] > 1:
    #             info_category.append({
    #                 "name": data_cat[0]["name"],
    #                 "category": search_category,
    #                 "score": data_cat[0]["score"],
    #                 "sku":  data_cat[0]["sku"]
    #             })
    #         else:
    #             info_category.append({
    #                 "name": None,
    #                 "category": search_category,
    #                 "score": 0,
    #                 "sku": None
    #             })
    #     else:
    #         info_category.append({
    #             "name": None,
    #             "category": search_category,
    #             "score": 0,
    #             "sku": None
    #         })

    # print(info_category)

compare_products(names=names)
