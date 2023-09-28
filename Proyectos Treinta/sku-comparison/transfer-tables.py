from google.cloud import bigquery
from google.cloud import secretmanager
from google.oauth2 import service_account
import pandas_gbq
import pymongo
import numpy as np
import pandas as pd

# Setting GCP credentials

credentials = service_account.Credentials.from_service_account_file(
    'data-development-337318-3c24c3bca95e.json', 
    scopes = [
        'https://www.googleapis.com/auth/cloud-platform',            
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery"
        ]
    )

project_id = 'data-development-337318'
secret_id = "TREINTA-DEV-MONGO"
version = "latest"
bq_client = bigquery.Client(credentials=credentials, project=project_id)
secret_client = secretmanager.SecretManagerServiceClient()

# Get the information from BQ tables

def sku_coinciden():

    sql_coinciden = """
    SELECT *
    FROM treintaco-sandbox.data_inputs.data_vw_temp_productos_coincidencia_sku
    """

    df_sku_coinciden = bq_client.query(sql_coinciden).to_dataframe()
    return df_sku_coinciden

def sku_no_coinciden():

    sql_no_coinciden = """
        SELECT *
        FROM treintaco-sandbox.data_inputs.data_vw_temp_productos_app_sin_coincidencia
        """

    df_sku_no_coinciden = bq_client.query(sql_no_coinciden).to_dataframe()
    return df_sku_no_coinciden

def access_secret_version(project_id, secret_id, version):

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"

    # Access the secret version.
    response = secret_client.access_secret_version(request={"name": name})

    payload = response.payload.data.decode("UTF-8")

    return payload

payload = access_secret_version(project_id=project_id, secret_id=secret_id, version=version)

# Send data to Mongodb
def send_df_coinciden(payload, df_sku_coinciden=sku_coinciden()):

    client = pymongo.MongoClient(payload)
    db = client["test_sku"]
    sku_coinciden = db["productos_coincidencia_sku"]

    df_sku_coinciden['deleted_at'] = pd.to_datetime(df_sku_coinciden['deleted_at'])
    df_sku_coinciden['deleted_at'] = df_sku_coinciden['deleted_at'].astype(object).where(df_sku_coinciden['deleted_at'].notnull(), None)
    df_sku_coinciden['created_at'] = pd.to_datetime(df_sku_coinciden['created_at'])
    df_sku_coinciden['created_at'] = df_sku_coinciden['created_at'].astype(object).where(df_sku_coinciden['deleted_at'].notnull(), None)
    df_sku_coinciden['_fivetran_synced'] = pd.to_datetime(df_sku_coinciden['_fivetran_synced'])
    df_sku_coinciden['_fivetran_syncedt'] = df_sku_coinciden['_fivetran_synced'].astype(object).where(df_sku_coinciden['deleted_at'].notnull(), None)
    df_sku_coinciden['updated_at'] = pd.to_datetime(df_sku_coinciden['updated_at'])
    df_sku_coinciden['updated_at'] = df_sku_coinciden['updated_at'].astype(object).where(df_sku_coinciden['deleted_at'].notnull(), None)
    df1_sku_coinciden = df_sku_coinciden.astype(object).where(pd.notnull(df_sku_coinciden),None)
    sku_coinciden_dict = df1_sku_coinciden.to_dict('records')

    for item in range(len(sku_coinciden_dict)):
        if len(sku_coinciden_dict[item]["product_data"]) > 0:
            sku_coinciden_dict[item]["product_data"] = sku_coinciden_dict[item]["product_data"].tolist()

    sku_coinciden.insert_many(sku_coinciden_dict)



# send_df_coinciden(payload=payload, df_sku_coinciden=sku_coinciden())

def send_df_no_coinciden(payload, df_sku_no_coinciden=sku_no_coinciden()):

    client = pymongo.MongoClient(payload)
    db = client["test_sku"]
    sku_no_coinciden = db["productos_app_sin_coincidencia"]

    df1_sku_no_coinciden = df_sku_no_coinciden.astype(object).where(pd.notnull(df_sku_no_coinciden),None)
    sku_no_coinciden_dict = df1_sku_no_coinciden.to_dict('records')

    for item in range(len(sku_no_coinciden_dict)):
        if len(sku_no_coinciden_dict[item]["product_data"]) >= 0:
            sku_no_coinciden_dict[item]["product_data"] = sku_no_coinciden_dict[item]["product_data"].tolist()

    print(sku_no_coinciden_dict[0])

    sku_no_coinciden.insert_many(sku_no_coinciden_dict)


send_df_no_coinciden(payload=payload, df_sku_no_coinciden=sku_no_coinciden())
