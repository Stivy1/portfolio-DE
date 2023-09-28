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

# Function to update clean_name in scrappers_clean

def clean_name_scrappers():

    regex = r'(?<=[a-zA-ZáéíóúÁÉÍÓÚñÑ])(?=\d)|(?<=\d)(?=[a-zA-ZáéíóúÁÉÍÓÚñÑ])'
    # Recorremos los documentos y actualizamos el campo 'clean_name'
    regex = r'([a-zA-Z]+)(\d+)'

    updates = []

    for doc in scrap_collection.find():
        if 'clean_name' in doc and isinstance(doc['clean_name'], str):
            original_name = doc['clean_name']
            updated_name = re.sub(regex, r'\1 \2', original_name)
            updated_name = re.sub(r'\s+', ' ', updated_name) # remove extra spaces
            if updated_name != original_name:
                updates.append(UpdateOne({'_id': doc['_id']}, {'$set': {'clean_name': updated_name}}))

    if updates:
        result = scrap_collection.bulk_write(updates)
        print(result.modified_count, 'documentos actualizados')
    else:
        print('No hay documentos para actualizar')

clean_name_scrappers()

#################### Function to get b2b_products ####################

def get_products_b2b(limit):

    pipeline_b2b = [
        {
            '$project': {
                '_id': 0,
                'name': 1,
                'clean_name': 1,
                'price': 1,
                'warehouse_name': 1,
                'ean': 1,
                'sku': 1
            }
        },
        {
            '$limit': limit
        }
    ]

    cursor = b2b_collection.aggregate(pipeline_b2b)
    products = []
    warehouses = []

    for doc in cursor:
        clean_name = clean_with_regex(str(doc['clean_name']))
        price = doc['price']
        warehouse_name = doc['warehouse_name']
        ean = doc['ean']
        sku = doc['sku']

        # Validar el nombre del warehouse y agregar a la lista si es uno de los especificados
        if any(word in warehouse_name.lower() for word in ['tuplaza', 'teresfoods', 'la campestre', 'nutreland', 'frigogher']):
            warehouses.append(warehouse_name)

        products.append({'clean_name': clean_name, 'price': price, 'warehouse_name': warehouse_name, 'ean': ean, 'sku': sku})

    return products

#################### Function to use a regular expression in order to separate de measurement units ####################

def clean_with_regex(clean_name):
    # Regular expression to separate letters and number
    if isinstance(clean_name, str):

        # Clean names
        cleaned_name = re.sub(r'[^a-zA-ZáéíóúÁÉÍÓÚñÑ0-9 ]+', '', clean_name)
        cleaned_name = re.sub(r'\s+', ' ', cleaned_name.strip())

        return cleaned_name
    else:
        return None

products = get_products_b2b(20000)

#################### Function to search products ####################

def search_products(products, batch_size=1000):

    num_batches = math.ceil(len(products) / batch_size)
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

    for i in range(num_batches):
        batch = products[i * batch_size: (i + 1) * batch_size]
        df_final_batch = []
        for product in tqdm(batch, desc=f'Searching products - Batch {i + 1} / {num_batches}'):
            try:
                to_search_name = product['clean_name']
                to_search_price = product['price']

                if product['warehouse_name'] in warehouses:
                    query_warehouses = [
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
                                    'should': {
                                        'near': {
                                            'path': 'price',
                                            'origin': to_search_price,
                                            'pivot': 0.5
                                        }
                                    }
                                }
                            }
                        },
                        {
                            '$match': {
                                'warehouse_name': 'Frubana'
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
                                'diff_price': {
                                    '$abs': {
                                        '$subtract': [
                                            '$price', to_search_price
                                        ]
                                    }
                                }
                            }
                        },
                        {
                            '$addFields': {
                                'score': {
                                    '$meta': 'searchScore'
                                }
                            }
                        },
                        {
                            '$setWindowFields': {
                                'output': {
                                    'maxScore': {
                                        '$max': '$score'
                                    }
                                }
                            }
                        },
                        {
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
                                'score': -1 , 
                                'diff_price': 1                  
                            }
                        }
                    ]

                    responses = db.scrappers_clean.aggregate(query_warehouses)
                    limit = 0

                    for response in responses:
                        df_final_batch.append({
                            'b2b_product': to_search_name,
                            'b2b_provider': product['warehouse_name'],
                            'b2b_price': to_search_price,
                            'b2b_ean': product['ean'],
                            'b2b_sku': product['sku'],
                            'assigned_product': response['clean_name'],
                            'assigned_provider': response['warehouse_name'],
                            'assigned_price': response['price'],
                            'assigned_ean': response['ean'],
                            'assigned_sku': response['sku'],
                            'score': response['score']
                        })

                        if limit >= 15:
                            break
                        limit += 1
                else:
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
                                    'should': {
                                        'near': {
                                            'path': 'price',
                                            'origin': to_search_price,
                                            'pivot': 0.5
                                        }
                                    }
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
                                'diff_price': {
                                    '$abs': {
                                        '$subtract': [
                                            '$price', to_search_price
                                        ]
                                    }
                                }
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
                            'b2b_provider': product['warehouse_name'],
                            'b2b_price': to_search_price,
                            'b2b_ean': product['ean'],
                            'b2b_sku': product['sku'],
                            'assigned_product': response['clean_name'],
                            'assigned_provider': response['warehouse_name'],
                            'assigned_price': response['price'],
                            'assigned_ean': response['ean'],
                            'assigned_sku': response['sku'],
                            'score': response['score'],
                            'last_sync': response['last_sync']
                        })

                        if limit >= 15:
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

search_products(products=products)

#################### Function to refine the data ####################

def refine_price():

    df = pd.read_csv('all_products_ean.csv')

    # Create a copy of the original dataframe to filter out empty rows

    total = []
    for _, row in tqdm(df.iterrows()):
        dif_price = (abs(row['b2b_price'] - row['assigned_price'])) / max(row['b2b_price'], row['assigned_price'])
        total.append(dif_price)

    df['dif_price_percent'] = total
    os.remove('all_products_ean.csv')
    df.to_csv('all_products_price_ean.csv', index=False)

refine_price()

#################### Function to obtaine the jaccard similarity ####################

def jaccard_similarity(b2b_product, assigned_product):

    intersection = len(list(set(b2b_product).intersection(assigned_product)))
    union = (len(b2b_product) + len(assigned_product)) - intersection
    return float(intersection) / union

#################### Function to obtaine Levensthein ####################

def levensthein_comparative(b2b_product, assigned_product, levensthaine_distance):
    if len(b2b_product) > len(assigned_product):
        return ((levensthaine_distance * 100) / len(b2b_product))
    else:
        return ((levensthaine_distance * 100) / len(assigned_product))

#################### Function to create a final file with the match products and metrics in order to evaluate the match ####################

def metrics_file():
    df = pd.read_csv("all_products_price_ean.csv")
    df['fuzzy_ratio'] = df.apply(lambda x: fuzz.ratio(str(x['b2b_product']), str(x['assigned_product'])), axis=1)
    df['fuzzy_token_set_ratio'] = df.apply(lambda x: fuzz.token_set_ratio(str(x['b2b_product']), str(x['assigned_product'])), axis=1)
    df['levenshtein_distance'] = df.apply(lambda x: Levenshtein.distance(str(x['b2b_product']), str(x['assigned_product'])), axis=1)

    print(df['b2b_product'][0])
    print('-------------------------------------')
    print(df['assigned_product'][0])
    df['jaccard_similarity'] = df.apply(lambda x: jaccard_similarity(x['b2b_product'], x['assigned_product']), axis=1)
    df['levensthein_comparative'] = df.apply(lambda x: levensthein_comparative(x['b2b_product'], x['assigned_product'], x['levenshtein_distance']), axis=1)

    print(jaccard_similarity(df['b2b_product'][0], df['assigned_product'][0]))
    df = df.dropna(subset=['fuzzy_ratio', 'fuzzy_token_set_ratio', 'levenshtein_distance', 'jaccard_similarity', 'score'])

    # Calcular la correlación de Pearson
    corr_fuzzy, _ = pearsonr(df['fuzzy_ratio'], df['score'])
    corr_fuzzy_token, _ = pearsonr(df['fuzzy_token_set_ratio'], df['score'])
    corr_levenshtein, _ = pearsonr(df['levenshtein_distance'], df['score'])
    corr_jaccard, _ = pearsonr(df['jaccard_similarity'], df['score'])
    corr_leven_comparative, _ = pearsonr(df['levensthein_comparative'], df['score'])

    # Imprimir los resultados
    print('Correlación con fuzzy_ratio:', corr_fuzzy)
    print('Correlación con levenshtein_distance:', corr_levenshtein)
    print('Correlación con jaccard_similarity:', corr_jaccard)
    print('Correlación con fuzzy_token_set_ratio:', corr_fuzzy_token)
    print('Correlación con levensthein:', corr_leven_comparative)

    os.remove("all_products_price_ean.csv")

    # Guardar el dataframe con las nuevas columnas
    df.to_csv('all_products_metrics_ean.csv', index=False)

metrics_file()

def refine_metrics():

    df = pd.read_csv('all_products_metrics_ean.csv')

    df.loc[(df['dif_price_percent'] >= 0.45), ['assigned_product', 'assigned_provider', 'assigned_price']] = ''
    df.loc[(df['dif_price_percent'] > 0.4) & (df['levensthein_comparative'] > 30), ['assigned_product', 'assigned_provider', 'assigned_price']] = ''
    df.loc[(df['dif_price_percent'] > 0.3) & (df['dif_price_percent'] <= 0.4) & (df['levensthein_comparative'] > 30), ['assigned_product', 'assigned_provider', 'assigned_price']] = ''
    df.loc[(df['dif_price_percent'] > 0.2) & (df['dif_price_percent'] <= 0.3) & (df['levensthein_comparative'] > 40), ['assigned_product', 'assigned_provider', 'assigned_price']] = ''
    df.loc[(df['dif_price_percent'] > 0.1) & (df['dif_price_percent'] <= 0.2) & (df['levensthein_comparative'] > 50), ['assigned_product', 'assigned_provider', 'assigned_price']] = ''
    df.loc[(df['dif_price_percent'] <= 0.1) & (df['levensthein_comparative'] > 60), ['assigned_product', 'assigned_provider', 'assigned_price']] = ''

    os.remove('all_products_metrics_ean.csv')

    df.to_csv('all_products_metrics_ean_total.csv', index=False)

    df_final = pd.read_csv('all_products_metrics_ean_total.csv')
    cols_especificas = ['assigned_product', 'assigned_provider', 'assigned_price']

    # Eliminar filas que contengan valores nulos en las columnas específicas
    df_sin_nulos = df_final.dropna(subset=cols_especificas, how='all')
    print(df_sin_nulos)

    os.remove('all_products_metrics_ean_total.csv')

    # Create a copy of the original dataframe to filter out empty rows

    df_sin_nulos.to_csv('all_products_metrics_ean_nan.csv', index=False)

refine_metrics()

def send_bq():
    df = pd.read_csv(
        'all_products_metrics_ean_nan.csv',
        dtype={
            'b2b_ean': 'str',
            'b2b_sku':'str',
            'b2b_price': 'int',
            'assigned_price': 'int',
            'assigned_ean': 'str',
            'assigned_sku':'str'
        },
        parse_dates=True
    )

    df_final = df.iloc[:,[0,1,2,3,4,5,6,7,8,9,11,12]]
    df_final['last_sync'] = pd.to_datetime(df_final['last_sync'])
    df_final['b2b_ean']= df_final['b2b_ean'].astype(str).str.replace('\.0$', '')
    df_final['assigned_ean']= df_final['assigned_ean'].astype(str).str.replace('\.0$', '')

    os.remove('all_products_metrics_ean_nan.csv')

    SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/bigquery.insertdata",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/cloud-platform.read-only",
    ]

    credentials, project = google.auth.default(
        scopes=SCOPES)
    
    client = bigquery.Client(credentials=credentials, project=project)

    table_id = 'data-production-337318.products_standarization.name_products_match_final'
    print('hola')
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("b2b_product", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("b2b_provider", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("b2b_price", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("b2b_ean", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("b2b_sku", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("assigned_product", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("assigned_provider", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("assigned_price", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("assigned_ean", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("assigned_sku", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("last_sync", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField("dif_price_percent", bigquery.enums.SqlTypeNames.FLOAT)            
        ],
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        df_final, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

send_bq()
