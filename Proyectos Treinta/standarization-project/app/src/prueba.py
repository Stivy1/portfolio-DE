from pymongo import MongoClient
from google.cloud import secretmanager, bigquery
from pymongo import UpdateOne
from tqdm import tqdm
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from fuzzywuzzy import fuzz
from scipy.stats import pearsonr
import google.auth
import pandas as pd
import re
import numpy as np
import os
import Levenshtein
import math

secret_id = "TREINTA-DEV-MONGO"
version = "latest"

secrets = secretmanager.SecretManagerServiceClient()
MONGO_URI = secrets.access_secret_version(request={"name": f"projects/692857860374/secrets/TREINTA-DEV-MONGO/versions/latest"}).payload.data.decode("utf-8")

mongo_client = MongoClient(MONGO_URI)
db = 'products'
db = mongo_client[db]
b2b_collection = db['b2b_products']
scrap_collection = db['scrappers_clean']


def search_products():

    # num_batches = math.ceil(len(products) / batch_size)
    df_total = []

    # print(products)

    warehouses = [
        'Bogotá - Tuplaza',
        'Pereira - TuPlaza',
        'Bogotá - La Campestre',
        'Cali - Tuplaza',
        'Bogotá - Nutreland',
        'Bogotá - Frigogher'
    ]

    # for i in range(num_batches):
    #     batch = products[i * batch_size: (i + 1) * batch_size]
    df_final_batch = []
    #for product in tqdm(batch, desc=f'Searching products - Batch {i + 1} / {num_batches}'):
    to_search_name = 'Jabon Rey'
        # to_search_price = product['price']
    try:
        to_search_name = 'Jabon Rey'
        # to_search_price = product['price']

        # if product['warehouse_name'] in warehouses:
        #     query_warehouses = [
        #         {
        #             '$search': {
        #                 'index': 'search_index',
        #                 'compound': {
        #                     'must': {
        #                         'text': {
        #                             'query': to_search_name,
        #                             'path': 'clean_name'
        #                         }
        #                     },
        #                     'should': {
        #                         'near': {
        #                             'path': 'price',
        #                             'origin': to_search_price,
        #                             'pivot': 0.5
        #                         }
        #                     }
        #                 }
        #             }
        #         },
        #         {
        #             '$match': {
        #                 'warehouse_name': 'Frubana'
        #             }
        #         },
        #         {
        #             '$project': {
        #                 'clean_name': 1,
        #                 'warehouse_name': 1,
        #                 'price': 1,
        #                 'ean': 1,
        #                 'sku': 1,
        #                 'last_sync': 1,
        #                 'score': {
        #                     '$meta': 'searchScore'
        #                 },
        #                 'diff_price': {
        #                     '$abs': {
        #                         '$subtract': [
        #                             '$price', to_search_price
        #                         ]
        #                     }
        #                 }
        #             }
        #         },
        #         {
        #             '$addFields': {
        #                 'score': {
        #                     '$meta': 'searchScore'
        #                 }
        #             }
        #         },
        #         {
        #             '$setWindowFields': {
        #                 'output': {
        #                     'maxScore': {
        #                         '$max': '$score'
        #                     }
        #                 }
        #             }
        #         },
        #         {
        #             '$addFields': {
        #                 'normalizedScore': {
        #                     '$divide': [
        #                         '$score', '$maxScore'
        #                     ]
        #                 }
        #             }
        #         },
        #         {
        #             '$sort': {
        #                 'score': -1 , 
        #                 'diff_price': 1                  
        #             }
        #         }
        #     ]

        #     responses = db.scrappers_clean.aggregate(query_warehouses)
        #     limit = 0

        #     for response in responses:
        #         df_final_batch.append({
        #             'b2b_product': to_search_name,
        #             'b2b_provider': product['warehouse_name'],
        #             'b2b_price': to_search_price,
        #             'b2b_ean': product['ean'],
        #             'b2b_sku': product['sku'],
        #             'assigned_product': response['clean_name'],
        #             'assigned_provider': response['warehouse_name'],
        #             'assigned_price': response['price'],
        #             'assigned_ean': response['ean'],
        #             'assigned_sku': response['sku'],
        #             'score': response['score']
        #         })

        #         if limit >= 30:
        #             break
        #         limit += 1
        # else:
        query = [
            {
                '$search': {
                    'index': 'search_index',
                    'compound': {
                        'must': {
                            'text': {
                                'query': to_search_name,
                                'path': 'clean_name'
                            }
                        },
                        # 'should': {
                        #     'near': {
                        #         'path': 'price',
                        #         'origin': to_search_price,
                        #         'pivot': 0.5
                        #     }
                        # }
                    }
                }
            },
            {
                '$project': {
                    'clean_name': 1,
                    'warehouse_name': 1,
                    'price': 1,
                    'ean': 1,
                    'sku': 1,
                    'last_sync': 1,
                    'score': {
                        '$meta': 'searchScore'
                    },
                    # 'diff_price': {
                    #     '$abs': {
                    #         '$subtract': [
                    #             '$price', to_search_price
                    #         ]
                    #     }
                    # }
                }
            },
            {
                '$addFields': {
                    'score': {
                        '$meta': 'searchScore'
                    }
                }
            }, {
                '$setWindowFields': {
                    'output': {
                        'maxScore': {
                            '$max': '$score'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'normalizedScore': {
                        '$divide': [
                            '$score', '$maxScore'
                        ]
                    }
                }
            },
            {
                '$sort': {
                    'score': -1,
                    'diff_price': 1                 
                }
            }
        ]

        responses = db.scrappers_clean.aggregate(query)
        limit = 0

        for response in responses:
            df_final_batch.append({
                'b2b_product': to_search_name,
                'assigned_product': response['clean_name'],
                'assigned_provider': response['warehouse_name'],
                'assigned_price': response['price'],
                'assigned_ean': response['ean'],
                'assigned_sku': response['sku'],
                'score': response['score'],
                'last_sync': response['last_sync']
            })

            if limit >= 30:
                break
            limit += 1

            
    except Exception as e: 
        print(e) 
    df_total_batch = pd.DataFrame(df_final_batch)
    df_total.append(df_total_batch)
    df_total = pd.concat(df_total)
    df_total.drop_duplicates(subset=['b2b_product', 'assigned_provider'], keep='first', inplace=True)

    df_total.to_csv('all_products_ean.csv', index=False)

    print(df_total)
    
search_products()