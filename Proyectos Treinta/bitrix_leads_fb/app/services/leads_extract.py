import random
from google.cloud import bigquery
import google

hunters_ids = [35, 33]

credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery"
    ])
client = bigquery.Client(credentials=credentials, project=project)

def asignar_numero():
    return random.choice(hunters_ids)

def extract_leads():
    query_update_lead= f"""
        SELECT
        A.*
        FROM `data-production-337318.bitrix_leads.facebook_leads_table` A
        LEFT JOIN `data-production-337318.bitrix_leads.facebook_leads_uploaded` B
        ON A.PHONE = B.PHONE
        AND A.TITLE = B.TITLE
        WHERE B.PHONE is null AND B.TITLE is null     
        """
    query_job = client.query(query_update_lead)
    df_BQ = query_job.to_dataframe()


    df_lead = df_BQ.fillna('')
    df_lead['ASSIGNED_BY_ID'] = df_lead.apply(lambda row: asignar_numero(), axis=1)

    return df_lead


