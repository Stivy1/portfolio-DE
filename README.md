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
