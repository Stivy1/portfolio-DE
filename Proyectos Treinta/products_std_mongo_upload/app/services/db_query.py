import pandas as pd
from google.cloud import bigquery
import google.auth

def query_df(query_str):
    credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/bigquery"
            ])
    client = bigquery.Client(credentials=credentials, project=project)

    
    query = """ {} """.format(query_str)
    query_job = client.query(query)
    df = query_job.to_dataframe()

    return df
