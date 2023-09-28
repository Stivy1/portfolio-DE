import requests
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import google.auth
from datetime import datetime
from fast_bitrix24 import Bitrix
import pandas as pd

webhook = "https://b24-hop273.bitrix24.co/rest/29/c1tkxna9k91oj120/"
b = Bitrix(webhook)

def extract_value(row, column_name):
    if column_name in row and isinstance(row[column_name], list):
        if len(row[column_name]) > 0:
            if 'VALUE' in row[column_name][0]:
                return row[column_name][0]['VALUE']
    return None

def get_method_list(method:str):
    data = b.get_all(method)
    df = pd.DataFrame(data)
    
    return df 

def get_by_id(df, method):
    consolidated_df = pd.DataFrame()
   
    list_IDS = df['ID'].to_list()
    list_IDS = [int(x) for x in list_IDS]
    data = b.get_by_ID(
        method,
        [d for d in list_IDS])

    df_new = pd.DataFrame(data)
    df_tr = df_new.transpose()
    date_columns = ['DATE_CREATE', 'DATE_REGISTER', 'DATE_MODIFY', 'LAST_UPDATED']  # Columnas de fecha que pueden variar
    users_columns = ['PHONE','EMAIL','WEB','IM']
    forms_columns = ['UF_CRM_1690230784', 'UF_CRM_1689637120616','UF_CRM_1689637086280', 'UF_CRM_1689636984854','UF_CRM_1689636855596','UF_CRM_1689636817918','UF_CRM_1689636761953', 'UF_CRM_1690227615', 'UF_CRM_1690918238677']
    client_forms_columns = ['UF_CRM_1690237948','UF_CRM_64B69067763BC','UF_CRM_64B690676B402','UF_CRM_64B690675F20E','UF_CRM_64B6906751FCE','UF_CRM_64B6906747D96', 'UF_CRM_64B690673BAC4', 'UF_CRM_1690239103', 'UF_CRM_1690925564']
    new_forms_columns = ['DETALLE_DIRECCION','ESTRATO', 'TIPO_CARTA', 'SOFTWARE', 'NUM_MESAS', 'FUENTE', 'TIPO_PLAN', 'RAZON_NO_COMPRA', 'STORE_ID']
    deal_forms_columns = ['UF_CRM_64C268EC92694','UF_CRM_1689615506053','UF_CRM_1689615565710','UF_CRM_64B6906809D7D','UF_CRM_64B6906819905','UF_CRM_64B690682323A', 'UF_CRM_64B690682D6FE', 'UF_CRM_64B690683663E', 'UF_CRM_64B69068408A3', 'UF_CRM_64C268ECA32A5', 'UF_CRM_1690927056151', 'UF_CRM_1690927099']
    new_deal_forms_columns = ['DETALLE_DIRECCION','TIPO_RESTAURANTE', 'TIPO_PLAN', 'DIRECCION', 'FUENTE', 'NUM_MESAS', 'SOFTWARE', 'TIPO_CARTA', 'ESTRATO', 'RAZON_NO_COMPRA', 'FECHA_CAPA_POSTVENTA', 'CAPACITADOR']

    for column in date_columns:
        if column in df_tr.columns:  
            df_tr[column] = pd.to_datetime(df_tr[column], format='%Y-%m-%dT%H:%M:%S%z').dt.strftime('%Y-%m-%d %H:%M:%S')
    columns_to_drop = ['TIMESTAMP_X', 'LAST_ACTIVITY_DATE', 'SETTINGS', 'PROVIDER_PARAMS']  # Columnas a eliminar
    columns_to_drop_existing = [col for col in columns_to_drop if col in df_tr.columns]

    if columns_to_drop_existing:
        df_tr.drop(columns_to_drop_existing, axis=1, inplace=True)    

    consolidated_df = pd.concat([consolidated_df, df_tr], ignore_index=True)
    
    for column_form in forms_columns:
        if column_form in consolidated_df.columns:        
            mapeo_columnas = dict(zip(forms_columns, new_forms_columns))
            consolidated_df.rename(columns=mapeo_columnas, inplace=True)
            consolidated_df[new_forms_columns] = consolidated_df[new_forms_columns].astype(str)
    
    for column_form in client_forms_columns:
        if column_form in consolidated_df.columns:        
            mapeo_columnas = dict(zip(client_forms_columns, new_forms_columns))
            consolidated_df.rename(columns=mapeo_columnas, inplace=True)
            consolidated_df[new_forms_columns] = consolidated_df[new_forms_columns].astype(str)
    
    for column_form in deal_forms_columns:
        if column_form in consolidated_df.columns:        
            mapeo_columnas = dict(zip(deal_forms_columns, new_deal_forms_columns))
            consolidated_df.rename(columns=mapeo_columnas, inplace=True)
            consolidated_df[new_deal_forms_columns] = consolidated_df[new_deal_forms_columns].astype(str)
    
    for column_user in users_columns:
        if column_user in consolidated_df.columns:  
            consolidated_df[column_user] = consolidated_df.apply(lambda row: extract_value(row, column_user), axis=1) 
    return consolidated_df

def get_BQ_df(table_id, df_api):
    # USAR PARA DESPLIEGUES
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/bigquery"
        ])
    client = bigquery.Client(credentials=credentials, project=project)

    date_columns = ['DATE_MODIFY', 'DATE_REGISTER', 'LAST_UPDATED']  # Columnas de fecha que pueden variar
    
    max_date_column = None
    
    for column in date_columns:
        query_check_column = f"""
        SELECT {column}
        FROM `{table_id}`
        LIMIT 1
        """
        try:
            client.query(query_check_column).result()
            max_date_column = column
            break
        except Exception:
            continue
    
    if max_date_column:
        query_max_BQ = f"""
        SELECT CAST(MAX(TIMESTAMP({max_date_column}, '+8:00')) AS STRING) as max_date_modify
        FROM `{table_id}`
        """
        query_job = client.query(query_max_BQ)
        df_BQ_max = query_job.to_dataframe()
        
        df_api = df_api[df_api[max_date_column] > df_BQ_max['max_date_modify'][0]]
        
        if not df_api.empty:
            ids = ",".join([f"'{str(row['ID'])}'" for _, row in df_api.iterrows()])
            query_BQ = f"""
            SELECT *
            FROM `{table_id}`
            WHERE ID IN ({ids})
            """
            query_job = client.query(query_BQ)
            df_BQ = query_job.to_dataframe()
            return df_BQ, df_BQ_max
        else:
            return pd.DataFrame(), df_BQ_max
    else:
        return False