from fastapi import FastAPI, UploadFile
import pandas as pd
import mysql.connector as msql
from mysql.connector import Error
from app.models.table_models import HiredEmployees, Departments, Jobs, generate_create_table_query
from app.core.logger import get_logger

logger = get_logger('API', "DEBUG")

app = FastAPI(title = "Upload Data and Check Metrics")

try:
    conn = msql.connect(
      host="localhost",
      user="root",
      password="stivy01",
      database="globant_data_challenge"
    )
    if conn.is_connected():
        logger.debug("Successful connection to database MySQL")
except Error as e:
    logger.error("Error while connecting to MySQL", e)


def get_model_class(table_name: str):
    table_to_class = {
        "HiredEmployees": HiredEmployees,
        "Departments": Departments,
        "Jobs": Jobs,

    }
    return table_to_class.get(table_name)


@app.post("/upload-csv/{table_name}")
async def upload_csv_to_mysql(table_name: str, file: UploadFile):
    if not table_name.isalnum():
        return {"error": f"The table {table_name} is not recognized as a valid table"}
        
    model_class = get_model_class(table_name)
    if model_class is None:
        return {"error": "Invalid table name"}

    data = pd.read_csv(file.file, header=None)
    data = data.fillna(0)

    cursor = conn.cursor()

    create_table_query_deparments, new_table_name = generate_create_table_query(model_class) 
    cursor.execute(create_table_query_deparments)
    logger.debug(f"Table {new_table_name} successfully created")

    columns = []
    for field_name, field_type in model_class.__annotations__.items():
        columns.append(field_name)
    try:
      for _, row in data.iterrows():
          data_dict = dict(zip(columns, row))
          
          query = f"INSERT INTO {new_table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
          
          values = tuple(data_dict[column] for column in columns)
          print(query, values)
          cursor.execute(query, values)
      conn.commit()
    except Exception as e:
      return {
                "status": 400,
                "message": f"Failed to upload the information {e}"
            }
  
    #cursor.close()
    #conn.close()
    return {
                "status": 200,
                "message": f"Data successfully loaded in the table {new_table_name}"
            }