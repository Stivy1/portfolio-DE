from pydantic import BaseModel
from typing import List
import stringcase

class HiredEmployees(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

class Departments(BaseModel):
    id: int
    deparment: str

class Jobs(BaseModel):
    id: int
    job: str
    
class EmployeesHiring2021(BaseModel):
    status: int 
    message: str
    hiredEmployees: list
    class Config:
        
        json_schema_extra = {
            "example": {
                "status": 200, 
                "message": "Data extracted succesful",
                "hiredEmployees":[{
                    "deparments": "Training",
                    "jobs": "Web Developer III",
                    "Q1": 0,
                    "Q2": 5,
                    "Q3": 1,
                    "Q4": 11,}]
            }
        }
        

class DepartmentsMoreHiring(BaseModel):
    status: int 
    message: str
    deparmentsMoreHiring: list
    class Config:
        
        json_schema_extra = {
            "example": {
                "status": 200, 
                "message": "Data extracted succesful",
                "deparmentsMoreHiring":[{
                    "id": 1,
                    "department": "Support",
                    "hired": 256}]
            }
        }

def generate_create_table_query(model_class):
    table_name = stringcase.snakecase(model_class.__name__)
    print(table_name)
    columns = []

    for field_name, field_type in model_class.__annotations__.items():
        if field_name == "id" and field_type == int:
            columns.append(f"{field_name} INT AUTO_INCREMENT PRIMARY KEY NOT NULL")
        else:
            sql_type = "INT" if field_type == int else "VARCHAR(255) NULL"
            columns.append(f"{field_name} {sql_type}")
    columns_str = ",\n".join(columns)
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_str}
        )
    """, table_name