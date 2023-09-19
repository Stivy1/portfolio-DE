from pydantic import BaseModel
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