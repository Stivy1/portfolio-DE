
from google.cloud import secretmanager
from google.cloud import bigquery
from google.oauth2 import service_account
import requests
import json
import os
import pandas as pd
from treintaLogger import *
import multiprocessing
from joblib import Parallel, delayed


#inicializamos el loger y las variables de entorno
logger = get_logger('Score API', "DEBUG")

#Creamos la funcion del secret manager para extraer los secrets de GCP y score
def access_secret_version(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    try:
        response = client.access_secret_version(name=name)
    except Exception as error:
        if str(error).find("403") >= 0:
            print('Permiso denegado')
    return response.payload.data.decode('utf-8')

#Se inician las variables que se usaran ams adelante
project_id = "692857860374"
login = json.loads(access_secret_version(project_id, "Score-login", version_id="latest"))
key_path = json.loads(access_secret_version(project_id, "SA-GCP-tables", version_id="latest"))
credentials = service_account.Credentials.from_service_account_info(
    key_path, scopes = ['https://www.googleapis.com/auth/cloud-platform', "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery",],
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
table_id = "treintaco-sandbox.score.users_loaded"
table_id_nl = 'treintaco-sandbox.score.users_not_loaded'
job_config = bigquery.LoadJobConfig(
            autodetect = True,
            create_disposition="CREATE_IF_NEEDED",
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            write_disposition="WRITE_APPEND",
        )

#Se crea la query con la que se extraeran los datos y se transforma en json
query_val = """select distinct "nd@T#!uX0l#fGMvUCc6uVVtD" as client_key,
                      store_id,
                      nombre_negocio,
                      direccion,
                      ciudad,
                      pais,
                      nombre_propietario,
                      telefono,
                      email,
                      campania,
                      is_score_lead,
                      tipo_tienda,
                      tipo_documento,
                      numero_documento,
                      numero_personas_negocio,
                      catalogo_virtual,
                      fecha_ultima_venta_treinta,
                      treinta_web,
                      datafono_treinta,
                      provedor_datafono_externo,
                      provedor_recargas_externo,
                      categoria_mas_vendido_ie,
                      dinero_balance_ingresos_extra,
                      dinero_disponible_billetera_treinta,
                      fecha_modificacion,
                      categoria_mas_comprada_b2b,
                      productos_mas_comprados_b2b,
                      productos_mas_vendido_ie,
                      categorias_mayor_registro_inventario,
                      productos_mas_vendidos_segun_treinta,
                      fecha_franja_lead, 
                      fecha_registro_treinta as fecha_registro_treinta
                    FROM `treintaco-sandbox.score.score_api`
                    where date(cast(fecha_registro_treinta as timestamp)) >= current_date('-5:00')-1
                    and store_id not in (select store_id from `treintaco-sandbox.score.users_loaded` WHERE)
                    order by fecha_registro_treinta desc"""
query_job = client.query(query_val)
df = query_job.to_dataframe()

df_json = df.to_json(orient='records')
body_json = json.loads(df_json)

url = 'https://api-treinta.scorecrm.pe/auth-login'
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}
body = login

response = requests.post(url, json=body, headers=headers)
text = response.text
text_clean = text[text.find('{'): text.find('}')+1]
response_json = json.loads(text_clean)
auth_token = response_json.get('auth_token')

#Se suben los datos mediante el API de score
url = 'https://api-treinta.scorecrm.pe/send-leads'
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": 'Bearer ' + auth_token
}

store_id = []
success = []
#Si por alguna razon falla, se guarda en una tabla de usuarios fallidos al subir.
for users in range(len(body_json)):
    response = requests.post(url, json=body_json[users], headers=headers)
    store_id.append(body_json[users]['store_id'])
    
    if response.json()['success'] == False:
        logger.warning(response.json())
        success.append(False)
    else:
        logger.debug(response.json())
        success.append(True)

df_validacion = pd.DataFrame({'store_id': store_id, 'success': success})

try:
    df = df.merge(df_validacion, on = 'store_id')
except ValueError:
    logger.warning("No hay datos para subir")
    exit()

df['fecha_subida'] = str(pd.Timestamp.now(tz='America/Bogota'))


df_fallidos = df[df.success == False]
df_fallidos.drop(columns = ['success'], inplace = True)
df_fallidos.reset_index(inplace = True, drop = True)

df_BQ = df[df.success == True]
df_BQ.drop(columns = ['success'], inplace = True)
df_BQ.reset_index(inplace = True, drop = True)

#Sube datos a loaded
job = client.load_table_from_dataframe(
    df_BQ,
    table_id,
    location="us-west1",
    job_config=job_config
)
job.result()

table = client.get_table(table_id)
logger.debug("Usuarios subidos: {}".format(len(df_BQ.index)))
logger.debug("Loaded {} rows to {}".format(
    len(df_BQ.index), table_id))

#sube datos a not loaded
if df_fallidos.empty == False:
    job = client.load_table_from_dataframe(
        df_fallidos,
        table_id_nl,
        location="us-west1",
        job_config=job_config
    )
    job.result()

    table = client.get_table(table_id_nl)
    logger.debug("Usuarios subidos: {}".format(len(df_fallidos.index)))
    logger.debug("Loaded {} rows to {}".format(
        len(df_fallidos.index), table_id_nl))
else:
    logger.debug("Ningun usuario con problemas")


fin = time.perf_counter()
logger.debug("!!!Envio Terminado!!!")
logger.debug("Usados {} minutos de tu tiempo.".format((fin - inicio)/60))