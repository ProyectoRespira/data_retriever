# data_retriever
 Creates a mirror from mysql remote database to local postgres database

## Setup
### Example .env file
Rename `.env.example` to `.env` and complete with credentials
```
POSTGRES_USER='fer'
POSTGRES_PASSWORD='nanda'
POSTGRES_HOST='localhost'
POSTGRES_DATABASE='estaciones'

MYSQL_USER='fer'
MYSQL_PASSWORD='nanda'
MYSQL_HOST='localhost'
MYSQL_DATABASE='estaciones_remote'
```