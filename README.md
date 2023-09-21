## Logbook

## Day 1

The whole Fast Api environment is created, the virtual environment, as well as the models of the required tables in Mysql.

A function is created in models to create tables from the database specifications.

We create the endpoint that reads the input csv using pandas.

Use a dynamic for to read the columns of the table to be uploaded to Mysql, and start iterating over the input dataframe to insert the records in the table. 

## Day 2

Endpoints are created to extract the required metrics.

Create the response models for each endpoint get.

A test folder is added where some unit tests are created for the validation of the endpoints.

A folder is added with the csv files that will be used to test the endpoints.

The database credentials are saved in a .env and added to the .gitignore.

A dockerfile slim file is created to deploy the code in a Docker container.

## Day 3

.gitignore and .dockerignore are created.

The Dockerfile is adapted to deploy in gcloud. 

Deployed successfully, you can see the details in the following link: https://console.cloud.google.com/run/detail/us-west1/globant-api/metrics?hl=es-419&project=globant-data-challenge

But the service appears unavailable: https://globant-api-xou3iwbnsq-uw.a.run.app/ =c

# Setup

To test the service, you must do the following:

## Step 1: Clone the repository.

Run the following command:

``` git clone https://github.com/Stivy1/portfolio-DE.git ```

## Step 2: Install dependencies with Poetry

First, install the virtual environment by running:

``` python3 -m venv venv ```

And activate

``` source venv/bin/activate ```

Next, install poetry

``` pip install poetry ```

Then update the packages

``` poetry update package ```

This will install the project dependencies.

## Step 3: add the .env file

To access the database, you must create the database in MySQL, and then put the credentials in an .env file
file with the following structure:
 
host="[host]"
user="[user]"
password="[pass]"
database="[database_name]"

## Step 4: Raise the service locally.

Now, run the following command to raise the service properly:

``` uvicorn app.main:app --reload ```.

This will bring up the service, and it will be accessible on the local host: 

http://127.0.0.1:8000/

To access the documentation, go to the following link:

http://127.0.0.1:8000/docs

## Testing in Docs

To test the data upload, you can find the csv files in the "input_files" folder and upload them in the /UploadCSV/{table_name} endpoint, where table_name should be the name specified in the documentation. 

This will create the required tables.

After creation, you can test the /getEmployeeHired2021 and /getDepartmentsMoreHiring endpoints.

## Pytest tests 

To run the tests, the pytest library must be installed:

``` pip install pytest ```

And then run the command (with the service still up):

```` pytest ```