from google.cloud import bigquery
import google.auth
import pandas as pd


def dataframe_to_BQ(data_frame: object, table_id: str, logger: object):
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform",
        ])
    client = bigquery.Client(credentials=credentials, project=project)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        write_disposition="WRITE_APPEND",
        )

    data_frame['upload_date'] = str(pd.Timestamp.now(tz='America/Bogota'))

    job = client.load_table_from_dataframe(
        data_frame,
        table_id,
        location="us-west1",
        job_config=job_config)

    job.result()

    debug = "Orders Uploaded: {0}. Loaded {0} rows to {1}".format(
            len(data_frame.index), table_id)

    return debug
