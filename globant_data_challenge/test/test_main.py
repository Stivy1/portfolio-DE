import pytest
from fastapi.testclient import TestClient
import os
from app.main import app
from io import BytesIO
import io
import requests

@pytest.fixture
def test_client():
    return TestClient(app)

@pytest.mark.parametrize("input_file_name", ["departments.csv", "hired_employees.csv", "jobs.csv"])
def test_upload_csv_to_mysql_correct_file(input_file_name):
    """
    Summary:
        This test try to send the corrects file to Mysql

    Args:
        input_file_name (bytes): The params is the files of the folder "input_files"
    """
    input_file_path = os.path.join(os.getcwd(), 'input_files', input_file_name)

    with open(input_file_path, 'rb') as file:
        file_contents = file.read()

    input_file = BytesIO(file_contents)

    api_url = "http://127.0.0.1:8000/UploadCSV/HiredEmployees"

    response = requests.post(api_url, files={"file": (input_file_name, input_file)})

    assert response.status_code == 200
    
    
def test_upload_csv_invalid_table(test_client):
    table_name = "InvalidTable"
    csv_data = "id,name,datetime,department_id,job_id\n1,John,2021-01-01,1,1\n2,Jane,2021-01-02,2,2"
    csv_file = io.BytesIO(csv_data.encode("utf-8"))
    response = test_client.post(f"/UploadCSV/{table_name}", files={"file": ("test.csv", csv_file)})
    assert response.json() == {"status": 400, "error": "Invalid table name"}
    
def test_get_employees_hired_2021(test_client):
    response = test_client.get("/getEmployeeHired2021")
    assert response.status_code == 200
    assert "hiredEmployees" in response.json()

def test_get_departments_more_hiring(test_client):
    response = test_client.get("/getDepartmentsMoreHiring")
    assert response.status_code == 200
    assert "deparmentsMoreHiring" in response.json()