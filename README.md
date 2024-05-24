# data_retriever
 Creates a mirror from mysql remote database to local postgres database
 
 ## Table of Contents

    Introduction
    Project Structure
    Installation
    Usage
    Contributing
    License

## Introduction

Provide a brief overview of the project, its purpose, and its main features.

## Current Project Structure

```
data_retriever/
│
├── src/
│   ├── database.py         # Contains functions to create sessions and engines
│   ├── initialize_db.py    # Script to initialize the database schema
│   ├── meteostat_data.py   # Module for fetching, processing and inserting meteostat data into localserver
│   ├── mirror.py           # Module for mirroring data between remoteserver (MySQL) and localserver (Postgres)
│   ├── models.py           # Contains database models
│   ├── stations_data.json  # JSON file containing station data
│   └── transform_raw_data.py  # Module for transforming raw data and inserting it into localserver
│
├── tests/
│   ├── conftest.py    
│   ├── test_main.py              # Unit tests for meteostat_data module
│   ├── test_meteostat_data.py    # Unit tests for meteostat_data module
│   ├── test_mirror.py            # Unit tests for mirror module
│   └── test_transform_raw_data.py  # Unit tests for transform_raw_data module
│
├── main.py           # Runs everything
├── .env              # Environment variables configuration file
├── README.md         # Project documentation
└── requirements.txt  # List of project dependencies
```

## Installation

Provide instructions on how to install and set up the project, including any dependencies or prerequisites.

## Usage

Explain how to use the project, including any command-line interfaces or APIs available.

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

## Contributing

Outline guidelines for contributing to the project, such as how to report issues, suggest improvements, or submit pull requests.

## License

Specify the license under which the project is distributed.
