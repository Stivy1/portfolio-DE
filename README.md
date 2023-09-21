# Bitacora

## Dia 1

Se crea todo el entorno de Fast Api, el entorno virtual, asi como los modelos de las tablas requeridas en Mysql

Se crea una funcion en models para crear tablas a partir de las especificaciones de la base de datos

Se crea el endpoint que lee el csv de entrada mediante pandas

Se usa un for dinamico para leer las columnas de la tabla que se va a subir a Mysql, y se empieza a iterar sobre el dataframe de entrada para insertar los registros en la tabla. 

## Dia 2

Se crean los Endpoints para extraccion de las metricas requeridas

Se crean los modelos de respuestas para cada endpoint get.

Se añade una carpeta test donde se crean algunos test unitarios para la validacion de los endpoints.

Se añade una carpeta con los csv que serviran para probar los endpoints.

Se guardan las credenciales de la base de datos en un .env y se añade al .gitignore

Se crea un archivo dockerfile slim para el despliegue del codigo en un contenedor Docker.

## Dia 3

Se crean los .gitignore y .dockerignore

Se adecua el archivo Dockerfile para hacer deploys en gcloud 

Se despliega con exito, se pueden ver los detalles en el siguiente link: https://console.cloud.google.com/run/detail/us-west1/globant-api/metrics?hl=es-419&project=globant-data-challenge

Pero el servicio aparece no disponible: https://globant-api-xou3iwbnsq-uw.a.run.app/

# Setup

Para probar el servicio, se debe hacer lo siguiente:

## Paso 1: Clonar el repositorio

Ejecutar el siguiente comando:

``` git clone https://github.com/Stivy1/portfolio-DE.git ```

## Paso 2: Instalar dependencias con Poetry

Primero, instalar el entrono virtual ejecutando:

``` python3 -m venv venv ```

Despues, instalar poetry

``` pip install poetry ```

Despues actualizar los paquetes

``` poetry update package ```

Esto instalara las dependencias del proyecto.

## Paso 3: añadir el archivo .env

Para acceder a la base de datos, se debe crear la base de datos en MySQL, y posteriormente poner las credenciales en un archivo .env
con la siguiente estructura:
 
host="<host>"
user="<user>"
password="<pass>"
database="<database_name>"

## Paso 4: Levantar el servicio en local.

Ahora, ejecutar el siguiente comando para levantar el servicio de manera correcta:

``` uvicorn app.main:app --reload ```

Esto levantara el servicio, y se podra acceder en el host local: 

http://127.0.0.1:8000/

Para acceder a la documentacion, ingresa al siguiente link:

http://127.0.0.1:8000/docs

## Pruebas en Docs

Para probar la subida de datos, puedes encontrar los archivos csv en la carpeta "input_files" y subirlas en el endpoint /UploadCSV/{table_name}, donde table_name debe ser el nombre especificado en la documentacion. 

Esto creara las tablas requeridas.

Posterior a la creacion, se puede probar los endpoints /getEmployeeHired2021 y /getDepartmentsMoreHiring

## Pruebas Pytest 

Para ejecutar las pruebas, se debe instalar la libreria pytest:

``` pip install pytest ```

Y posteriormente, ejecutar el comando (Tener aun el servicio levantado):

``` pytest ```