# data_retriever
 Creates a mirror from mysql remote database to local postgres database

## Setup
### Example database.ini file
```
[postgresql]
HOST = localhost
USER = your_user
PASSWORD = your_password
DATABASE = ESTACIONES_MIRROR
TABLES = Estacion1, Estacion2, Estacion3, Estacion4, Estacion5, Estacion6, Estacion7, Estacion8, Estacion9, Estacion10

[mysql]
HOST = remote_msql_server
USER = remote_user
PASSWORD = remote_password
DATABASE = Estaciones
TABLES = Estacion1, Estacion2, Estacion3, Estacion4, Estacion5, Estacion6, Estacion7, Estacion8, Estacion9, Estacion10
```

