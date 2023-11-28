import mysql.connector
import pandas as pd

def mysql_upload(df: object, table_name: str):
    conn = mysql.connector.connect(host = 'localhost', user = 'Stivy', passwd = 'Stivy01*', db = 'steam', auth_plugin='mysql_native_password')
    cursor = conn.cursor()

    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    exist_table = cursor.fetchone()
    if not exist_table:
        cursor.execute(f"CREATE TABLE {table_name} ("
                    f"{', '.join([f'{column} VARCHAR(255)' for column in df.columns])}"
                    f")")

    for _, row in df.iterrows():
        values = ', '.join([f"'{value}'" for value in row.values])
        cursor.execute(f"INSERT INTO {table_name} VALUES ({values})")



