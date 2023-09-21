from fastapi import FastAPI, UploadFile
from fastapi.responses import JSONResponse
import pandas as pd
import mysql.connector as msql
from mysql.connector import Error
from app.models.models import HiredEmployees, Departments, Jobs, DepartmentsMoreHiring, EmployeesHiring2021, generate_create_table_query
import json
from app.core.logger import get_logger
import os
from dotenv import load_dotenv

logger = get_logger('API', "DEBUG")

app = FastAPI(title = "Upload Data and Check Metrics")

load_dotenv()

HOST= os.getenv('host')
USER=os.getenv('user')
PASSWORD=os.getenv('password')
DATABASE=os.getenv('database')

try:
    conn = msql.connect(
      host=HOST,
      user=USER,
      password=PASSWORD,
      database=DATABASE
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


@app.post("/UploadCSV/{table_name}")
async def upload_csv_to_mysql(table_name: str, file: UploadFile):
    """
    Summary:
        This function receives a CSV file and the name of a table, to upload the information to a MySQL database. 
        
        
    Args:
        table_name (str): The table name must exist in the table models: HiredEmployees, Departments, Jobs. \n
        file (UploadFile): The file must be .csv

    Returns:
        200: If the data is uploaded successfully, it will print the sentences that upload the data and a success message.
        400: If an error was made in the process.
    """
    
    if not table_name.isalnum():
        return {
                "status": 400, 
                "error": f"The table {table_name} is not recognized as a valid table"
            }
        
    model_class = get_model_class(table_name)
    if model_class is None:
        return {
                "status": 400, 
                "error": "Invalid table name"
            }

    data = pd.read_csv(file.file, header=None)
    if data.empty == True:
        return {
                "status": 400, 
                "error": "Empty Dataframe"
            }

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
    

@app.get("/getEmployeeHired2021", response_model=EmployeesHiring2021, response_class=JSONResponse)
async def get_employees_hired_2021():
    """
    Summary:
        This GET function returns a JSON String containing the information of the employees hired in the 4 quarters of the year 2021, grouped by departments and jobs. 
        

    Returns:
        Return 200: If the data is extracted successfully, it will print the JSON string and a success message.
        Return 400: If an error was made in the process.
    """
    
    query = """ SELECT
                    d.deparment as deparments,
                    j.job as jobs, 
                    COUNT(CASE 
                        WHEN date(datetime) BETWEEN "2021-01-01" AND "2021-03-31" THEN he.id ELSE null END) AS "Q1",
                    COUNT(CASE 
                        WHEN date(datetime) BETWEEN "2021-04-01" AND "2021-06-30" THEN he.id ELSE null END) AS "Q2",
                    COUNT(CASE 
                        WHEN date(datetime) BETWEEN "2021-07-01" AND "2021-09-30" THEN he.id ELSE null END) AS "Q3",
                    COUNT(CASE 
                        WHEN date(datetime) BETWEEN "2021-10-01" AND "2021-12-31" THEN he.id ELSE null END) AS "Q4"    
                    
                FROM hired_employees he
                JOIN departments d
                    ON he.department_id = d.id
                JOIN jobs j
                    ON he.job_id = j.id

                WHERE date(datetime) BETWEEN "2021-01-01" AND "2021-12-31"
                GROUP BY 1,2
                ORDER BY d.deparment DESC, j.job  DESC"""
    try:
        print(query)
        df = pd.read_sql(query, conn)
        logger.debug(df)
        result = json.loads(df.to_json(orient='records'))
        print(result)
        return {"status": 200, "message": "Data extracted succesful", "hiredEmployees": result}
    except:
        return {"status": 400, "message": "Failed to extract the information. Check the Query", "hiredEmployees": []}

@app.get("/getDepartmentsMoreHiring",  response_model=DepartmentsMoreHiring, response_class=JSONResponse)
async def get_deparments_more_hiring():
    """
    Summary:
        This GET function returns a JSON String containing the information of the deparments with more hiring employees. 
        

    Returns:
        Return 200: If the data is extracted successfully, it will print the JSON string and a success message.
        Return 400: If an error was made in the process.
    """
    
    query = """ WITH MEAN_2021 AS (
                    SELECT AVG(hired_employees) AS AVG_EMPLOYEES
                    FROM (
                        SELECT 
                            D.deparment,
                            COUNT(HE.ID) AS hired_employees
                        FROM hired_employees HE
                        JOIN departments D
                        ON HE.department_id = D.id

                        WHERE DATE(HE.datetime) >= "2021-01-01" AND "2021-12-31"
                        GROUP BY 1
                    ) AS CE
                    )

                    SELECT 
                        D.id,
                        D.deparment,
                        COUNT(HE.id) AS hired
                    FROM hired_employees HE
                    JOIN departments D
                    ON HE.department_id = D.id
                    GROUP BY 1,2
                    HAVING hired > (SELECT AVG_EMPLOYEES FROM MEAN_2021)
                    ORDER BY hired DESC"""
    try:
        df = pd.read_sql(query, conn)
        result = json.loads(df.to_json(orient='records'))
        print(result)
        return {"status": 200, "message": "Data extracted succesful", "deparmentsMoreHiring": result}
    except:
        return {"status": 400, "message": "Failed to extract the information. Check the Query", "deparmentsMoreHiring": []}    