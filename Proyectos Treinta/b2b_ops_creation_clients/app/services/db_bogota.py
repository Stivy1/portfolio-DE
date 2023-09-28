import pandas as pd
from google.cloud import bigquery
import google.auth
from services.secret_manager import access_secret_version
import mysql.connector

def get_last_batch_bog(logger: object):
  #USAR PARA DESPLIEGUES
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/bigquery"
        ])
    client = bigquery.Client(credentials=credentials, project=project)
    project_id = "555800018042"
    creds_BD  = access_secret_version(project_id, "DATA_MYSQL_DB_DEV", version_id="latest")

    mydb  = mysql.connector.connect(
        host=creds_BD['DB_HOST'],
        user=creds_BD['DB_USERNAME'],
        password=creds_BD['DB_PASSWORD'],
        database=creds_BD['DB_NAME']
    )
    
    mycursor = mydb.cursor()

    query = """
    SELECT DISTINCT
        tipo_de_identificacion,
        numero_de_documento,
        documento_limpio,
        TRIM(nombre_del_cliente) as nombre_del_cliente,
        TRIM(nombre_establecimiento) as nombre_establecimiento,
        '' as foto_cedula,
        celular,
        correo,
        CASE WHEN cliente_institucional = "false" THEN "NO"
            WHEN cliente_institucional = "true" THEN "SI"
            ELSE cliente_institucional END AS cliente_institucional,
        ciudad,
        tipo_negocio,
        direccion as direccion,
        latitud,
        longitud,
        fecha_entrega,
        ultima_fecha_orden
    FROM `data-outputs`.b2b_ops_clients
    WHERE check_ops IS TRUE
    AND proveedor in ("TROPI", "TROPI INSTITUCIONAL")
    AND fecha_entrega >= "2023-05-10"
    """
    mycursor.execute(query)
    results = mycursor.fetchall()

    df_mysql = pd.DataFrame(results, columns=[i[0] for i in mycursor.description])
    print(df_mysql)
    query_BQ = """
    SELECT numero_de_documento FROM `data-production-337318.b2b_ops.clients_uploaded` """
    query_job = client.query(query_BQ)
    df_BQ = query_job.to_dataframe()

    if df_BQ.empty is True:
        logger.info(df_BQ)
        logger.warning("No hay datos para extraer bogota")
        return pd.DataFrame()
    
    df_filtered = df_mysql[~df_mysql['numero_de_documento'].isin(df_BQ['numero_de_documento'])]
    return df_filtered
