from google.cloud import bigquery
import google.auth
import pandas as pd
import re
credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform",
        ])
client = bigquery.Client(credentials=credentials, project=project)
#Truncate
def dataframe_to_BQ(data_frame: object, table_id: str):
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        )

    job = client.load_table_from_dataframe(
        data_frame,
        table_id,
        location="us-west1",
        job_config=job_config)

    job.result()

    debug = "Data Uploaded: {0}. Loaded {0} rows to {1}".format(
            len(data_frame.index), table_id)

    return debug
#Insert
def append_to_BQ(data_frame: object, table_id: str):
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        write_disposition="WRITE_APPEND",
        )

    job = client.load_table_from_dataframe(
        data_frame,
        table_id,
        location="us-west1",
        job_config=job_config)

    job.result()

    debug = "Data appended: {0}. Loaded {0} rows to {1}".format(
            len(data_frame.index), table_id)

    return debug
#Update
def update_to_BQ(df_upd:object, table_id: str):
    df_upd.fillna(pd.NA, inplace=True)
    df_upd.replace({pd.NA: "null"}, inplace=True)
    df_upd = df_upd.applymap(lambda x: re.sub(r'\r?\n', '', str(x)) if pd.notna(x) else x)
    update_queries = []
    for _, row in df_upd.iterrows():
        set_clauses = ",\n".join([f"`{table_id}`.{column} = '{row[column]}'" for column in df_upd.columns])
        update_query = f"""
        UPDATE `{table_id}`
        SET
        {set_clauses}
        WHERE ID = '{str(row['ID'])}'
        """
        update_query = update_query.replace("'null'", "null")
        update_queries.append(update_query)
        
    query_job_config = bigquery.QueryJobConfig()
    
    for query in update_queries:
        query_job = client.query(query, job_config=query_job_config)
        query_job.result()
        
    debug = "Data Updated: {0}. Loaded {0} rows to {1}".format(
        len(df_upd.index), table_id)

    return debug
#Historic
def append_historic_to_BQ(data_frame: object, table_id: str):
    
    table_id = table_id + "_historic"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        write_disposition="WRITE_APPEND",
        )

    job = client.load_table_from_dataframe(
        data_frame,
        table_id,
        location="us-west1",
        job_config=job_config)

    job.result()

    debug = "Data appended: {0}. Loaded {0} rows to {1}".format(
            len(data_frame.index), table_id)

    return debug


#delete
def delete_to_BQ(df_del:object, table_id: str):

    delete_query = f"""
    DELETE `{table_id}`
    WHERE ID IN ({','.join([f"'{str(row['ID'])}'" for _, row in df_del.iterrows()])})
    """
    
    query_job_config = bigquery.QueryJobConfig()
    query_job = client.query(delete_query, job_config=query_job_config)
    query_job.result()
    debug = "Data deleted: {0}. Loaded {0} rows to {1}".format(
            len(df_del.index), table_id)

    return debug