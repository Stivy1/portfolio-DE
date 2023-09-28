from fastapi import UploadFile
from db.base import db
from models.gtins import BaseProduct
from core.config import settings
from io import BytesIO
from datetime import datetime
import requests
import pandas as pd
import json
from google.cloud import bigquery

PRODUCTS_COLLECTION = db.get_collection("sku_product_information")


class crud_gtins:
    async def get_sku_by_file(file: UploadFile):

        contents = await file.read()
        buffer = BytesIO(contents)
        sku = pd.read_csv(buffer)
        if "sku" in sku:
            response_sku = sku['sku'].to_list()
            result_sku = [str(i) for i in response_sku]
        else:
            return {
                "status": 400,
                "message": "No existe la columna sku en el dataframe"
            }
        
        if len(result_sku) > 0:
            return {
                "status": 200,
                "data": result_sku
            }
        else:
            return {
                "status": 400,
                "message": "La lista de sku está vacía"
            }

    async def get_token():

        url_login = settings.LOGYCA_LOGIN_URL

        payload = {
            "email": settings.LOGYCA_USER,
            "password": settings.LOGYCA_PASSWORD
        }

        response = requests.request("POST", url_login, json=payload)
        res = response.json()
        if "token" in res:
            return {
                "status": 200,
                "data": res["token"]
            }
        else:
            return {
                "status": 400,
                "message": res["message"]
            }

    async def get_commons_sku(token, sku):

        url_gtins = settings.LOGYCA_GTINS_URL
        headers = {
            "x-access-token": token
        }

        response_gtins = requests.request("POST", url_gtins, headers=headers, json=sku)
    
        status = response_gtins.status_code
        data = response_gtins.json()
        if status != 200:
            return {
                "status": 400,
                "message": "No se encontraron coincidencias de sku de treinta en la API de LOGYCA"
            }
        
        commons_sku = []
        if len(data) > 1:
            for i in data:
                if "validationErrors" not in i:                
                    mapping = {
                        "sku": int(i["gtin"]),
                        "category": int(i["gpcCategoryCode"]),
                        "brand": i["brandName"][0]["value"],
                        "name": i["productDescription"][0]["value"],
                        "description": i["productDescription"][0]["value"],
                        "is_cached": False,
                        "update_date": datetime.now()           
                    }
                    if "netContent" in i:
                        mapping["unit_name"] =  i["netContent"][0]["unitCode"]
                        mapping["unit_quantity"] =  float(i["netContent"][0]["value"])
                    if "countryOfSaleCode" in i:  
                        mapping["sale_country"] = i["countryOfSaleCode"][0]["alpha2"]
                    if  "language" in i["brandName"][0]:
                        mapping["language"] = i["brandName"][0]["language"]

                    commons_sku.append(mapping)
        else:
            mapping = {
                "sku": int(data[0]["data"]["gtin"]),
                "category": int(data[0]["data"]["gpcCategoryCode"]),
                "brand": data[0]["data"]["brandName"][0]["value"],
                "name": data[0]["data"]["productDescription"][0]["value"],
                "description": data[0]["data"]["productDescription"][0]["value"],
                "is_cached": False,
                "update_date": datetime.now()           
            }
            if "netContent" in data[0]["data"]:
                mapping["unit_name"] =  data[0]["data"]["netContent"][0]["unitCode"]
                mapping["unit_quantity"] =  float(data[0]["data"]["netContent"][0]["value"])
            if "countryOfSaleCode" in data[0]["data"]:  
                mapping["sale_country"] = data[0]["data"]["countryOfSaleCode"][0]["alpha2"]
            if  "language" in data[0]["data"]["brandName"][0]:
                mapping["language"] = data[0]["data"]["brandName"][0]["language"]

            commons_sku.append(mapping)        

        if len(commons_sku) > 0:
            return {
                "status": 200,
                "data": commons_sku
            }
        else:
            return {
                "status": 400,
                "message": "No se encontraron coincidencias de sku de treinta en la API de LOGYCA"
            }
    
    async def rename_fields(commons_sku):

        units_code_file = open("./units_code.json")
        units_code = json.load(units_code_file)

        categories_file = open("./categories.json")
        categories = json.load(categories_file)
        
        product_mapping = []
        for sku in commons_sku:
            for code in units_code:
                if "unit_name" in sku:
                    if sku["unit_name"] == code["code"]:
                        sku["unit_name"] = code["c"]
            for category in categories:
                if sku["category"] == int(category["brick_code"]):
                    if "language" in sku:
                        if sku["language"] == "pt-BR":
                            sku["category"] = category["brick_description"]
                        else:
                            sku["category"] = category["spanish_name_brick"]

            product_mapping.append(sku)

        dataframe = pd.DataFrame(
            product_mapping,
            columns=[
               "sku",
               "category",
               "brand",
               "name",
               "description",
               "unit_name",
               "unit_quantity",
               "sale_country",
               "language",
               "is_cached",
               "update_date"
            ]
        )

        product_mapping_df = dataframe.astype({
            'sku': 'int64', 
            'category': 'str', 
            'brand': 'str', 
            'name': 'str',
            'description': 'str',
            'unit_name': 'str',
            'unit_quantity': 'float64',
            'sale_country': 'str',
            'language': 'str',
            'is_cached': 'bool',
            'update_date': 'datetime64'
            
        })

        if len(product_mapping_df) > 0:
            return {
                "status": 200,
                "data": {
                    "df": product_mapping_df,
                    "object": product_mapping
                }
            }
        else:
            return {
                "status": 400,
                "message": "No existen productos para guardar"
            }

    async def save_mongo(products):
   
        sku_list = []
        for product in products:
            sku_list.append(product["sku"])
        valid_sku = [item async for item in PRODUCTS_COLLECTION.find({"sku": {"$in": sku_list}}, {"_id": 0, "sku": 1})]
        new_product = []
        for product in products:
            exist = False
            for sku_stored in valid_sku:
                if product["sku"] == sku_stored["sku"]:
                    exist = True
            if exist == False:
                new_product.append(product)

        if len(new_product) > 0:
            await PRODUCTS_COLLECTION.insert_many(new_product)
            return {
                "status": 200,
                "message": "Producto guardado con éxito",
                "data": new_product
            }
        else:
            return {
                "status": 400,
                "message": "Los productos ya se encuentran en la base de datos"
            }

    def json_to_df(new_product):

        dataframe = pd.DataFrame(
            new_product,
            columns=[
               "sku",
               "category",
               "brand",
               "name",
               "description",
               "unit_name",
               "unit_quantity",
               "sale_country",
               "language",
               "is_cached",
               "update_date"
            ]
        )

        new_product_mapping_df = dataframe.astype({
            'sku': 'int64', 
            'category': 'str', 
            'brand': 'str', 
            'name': 'str',
            'description': 'str',
            'unit_name': 'str',
            'unit_quantity': 'float64',
            'sale_country': 'str',
            'language': 'str',
            'is_cached': 'bool',
            'update_date': 'datetime64'     
        })
        return new_product_mapping_df

                
    async def save_bq(new_product_mapping_df):

        dictionary = {
            "type": settings.TYPE,
            "project_id": settings.PROJECT_ID_DEV,
            "private_key_id": settings.PRIVATE_KEY_ID,
            "private_key": settings.PRIVATE_KEY,
            "client_email": settings.CLIENT_EMAIL,
            "client_id": settings.CLIENT_ID,
            "auth_uri": settings.AUTH_URI,
            "token_uri": settings.TOKEN_URI,
            "auth_provider_x509_cert_url": settings.AUTH_PROVIDER,
            "client_x509_cert_url": settings.CLIENT_URL
        }
        with open("cred.json", "w") as outfile:
            json.dump(dictionary, outfile)

        client = bigquery.Client.from_service_account_json("cred.json")

        job_config = bigquery.LoadJobConfig(
            create_disposition = "CREATE_IF_NEEDED",
            write_disposition = "WRITE_APPEND")

        job = client.load_table_from_dataframe(
            new_product_mapping_df, settings.TABLE_ID, job_config=job_config
        )
        job.result()

        sku_product_information = client.get_table(settings.TABLE_ID)  
        print(
            "Loaded {} rows and {} columns to {}".format(
                sku_product_information.num_rows, len(sku_product_information.schema), settings.TABLE_ID
            )
        )

        if new_product_mapping_df.size > 0:
            return {
                "status": 200,
                "data": "Productos guardados en BigQuery"
            }
        else:
            return {
                "status": 400,
                "message": "No existen productos para guardar"
            }

    async def update_mongo(sku: int, req):
        product = await PRODUCTS_COLLECTION.find_one({"sku": sku})
        if product:
            if {
                "category" in req or
                "brand" in req or
                "name" in req or 
                "description" in req or
                "unit_name" in req or
                "unit_quatity" in req or
                "sale_country" in req or
                "language" in req or
                "is_cached" in req or
                "update_date" in req
            }:
                response_update = PRODUCTS_COLLECTION.update_one({"sku": sku}, {"$set": req})
            
                if response_update:
                    return {
                        "status": 200,
                        "message": "Producto actualizado correctamente"
                    }
        else:
            return {
                "status": 400,
                "message": "¡El producto no existe!"
            }
        
    def update_bq(sku: int, req):
        keys = req.keys()
        query = '''UPDATE `treintaco-sandbox.products.sku_product_information` SET '''
        for idx, i in enumerate(keys):
            query += i
            query += ' = '
            if i == 'update_date':
                query += "DATETIME('" + str(req[i]) + "')"
            else:
                query += str(req[i])
            if idx < len(keys) - 1:
                query += ', '
        query += ' WHERE sku=' + str(sku)

        dictionary = {
            "type": settings.TYPE,
            "project_id": settings.PROJECT_ID_DEV,
            "private_key_id": settings.PRIVATE_KEY_ID,
            "private_key": settings.PRIVATE_KEY,
            "client_email": settings.CLIENT_EMAIL,
            "client_id": settings.CLIENT_ID,
            "auth_uri": settings.AUTH_URI,
            "token_uri": settings.TOKEN_URI,
            "auth_provider_x509_cert_url": settings.AUTH_PROVIDER,
            "client_x509_cert_url": settings.CLIENT_URL
        }
        with open("cred.json", "w") as outfile:
            json.dump(dictionary, outfile)

        client = bigquery.Client.from_service_account_json("cred.json")

        query_job = client.query(query)

        results = query_job.result()
        return {
            "status": 200,
            "message": "Producto actualizado correctamente"
        }

    async def delete_mongo(sku:int):
        product = PRODUCTS_COLLECTION.find_one({"sku": sku})
        if product:
            PRODUCTS_COLLECTION.delete_one({"sku": sku})
            return {
                "status": 200,
                "message": "¡Producto eliminado correctamente!"
            }
        return {
            "status": 400,
            "message": "El producto no existe"
        }

    def delete_bq(sku: int):

        dictionary = {
            "type": settings.TYPE,
            "project_id": settings.PROJECT_ID_DEV,
            "private_key_id": settings.PRIVATE_KEY_ID,
            "private_key": settings.PRIVATE_KEY,
            "client_email": settings.CLIENT_EMAIL,
            "client_id": settings.CLIENT_ID,
            "auth_uri": settings.AUTH_URI,
            "token_uri": settings.TOKEN_URI,
            "auth_provider_x509_cert_url": settings.AUTH_PROVIDER,
            "client_x509_cert_url": settings.CLIENT_URL
        }
        with open("cred.json", "w") as outfile:
            json.dump(dictionary, outfile)

        client = bigquery.Client.from_service_account_json("cred.json")

        query = '''DELETE FROM `treintaco-sandbox.products.sku_product_information`
        WHERE sku = ''' + str(sku)
        print(query)

        query_job = client.query(query)

        results = query_job.result()
        return {
            "status": 200,
            "message": "¡Producto eliminado correctamente!"
        }

    async def list_products(page, size):
        skip = (int(page) - 1) * int(size)
        data = [item async for item in PRODUCTS_COLLECTION.find({}, {'_id': 0}).skip(skip).limit(int(size))]
        return {
            "status": 200,
            "data": data
        }

    async def get_product_by_sku(sku: int):
        async for product in PRODUCTS_COLLECTION.find({"sku": sku}, {'_id': 0}):
            if product:
                return {
                    "status": 200,
                    "data": BaseProduct(**product),
                    "message": "Información del producto"
                }
        return {
            "status": 400,
            "message": "El producto no existe",
            "data": ""
        }

    async def create_products(products):
        sku_list = []
        for product in products:
            sku_list.append(product["sku"])
        valid_sku = [item async for item in PRODUCTS_COLLECTION.find({"sku": {"$in": sku_list}}, {"_id": 0, "sku": 1})]
        new_products = []
        for product in products:
            exist = False
            for sku_stored in valid_sku:
                if product["sku"] == sku_stored["sku"]:
                    exist = True
            if exist == False:
                new_products.append(product)
        for product in new_products:
            if "sku" not in product:
                return {
                    "status": 400,
                    "message": "El campo sku es necesario para crear un nuevo producto"
                }
            if "category" not in product:
                return {
                    "status": 400,
                    "message": "El campo category es necesario para crear un nuevo producto"
                }
            if "brand" not in product:
                return {
                    "status": 400,
                    "message": "El campo brand es necesario para crear un nuevo producto"
                }
            if "name" not in product:
                return {
                    "status": 400,
                    "message": "El campo name es necesario para crear un nuevo producto"
                }
            if "description" not in product:
                return {
                    "status": 400,
                    "message": "El campo description es necesario para crear un nuevo producto"
                }
            if "is_cached" not in product:
                return {
                    "status": 400,
                    "message": "El campo is_cached es necesario para crear un nuevo producto"
                }
            if "update_date" not in product:
                return {
                    "status": 400,
                    "message": "El campo update_date es necesario para crear un nuevo producto"
                }

        if len(new_products) > 0:
            await PRODUCTS_COLLECTION.insert_many(new_products)
            return {
                "status": 200,
                "messsage": "Nuevos productos agregados",
                "data": new_products
            }
        else:
            return {
                "status": 200,
                "message": "Los productos ya se encuentran en la base de datos",
                "data": []
            }
        
        

gtins = crud_gtins()
