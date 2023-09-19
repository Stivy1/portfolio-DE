## Dia 1

Se crea todo el entorno de Fast Api, el entorno virtual, asi como los modelos de las tablas requeridas en Mysql

Se crea una funcion en models para crear tablas a partir de las especificaciones de la base de datos

Se crea el endpoint que lee el csv de entrada mediante pandas

Se usa un for dinamico para leer las columnas de la tabla que se va a subir a Mysql, y se empieza a iterar sobre el dataframe de entrada para insertar los registros en la tabla. 
