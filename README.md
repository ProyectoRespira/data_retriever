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
│   ├── models.py           # Contains database models
│   ├── stations_data.json  # JSON file containing station data
│   ├── region_data.json    # JSON file containing region data
│   ├── time_utils.py       # time conversion functions
│   ├── features/                # code to build features for inference
│   │   ├── utils.py            
│   │   └── extract_data.py     # Functions to build features
│   ├── calibration/              # code to calculate calibration factors
│   │   ├── utils.py            
│   │   └── transform_data.py   # Functions to load calibration factors to db
│   ├── extract/                # code to extract data from various sources
│   │   ├── utils.py            
│   │   └── extract_data.py     # Functions to extract data
│   ├── transform/              # code to transform incoming data from various sources
│   │   ├── utils.py            
│   │   └── transform_data.py   # Functions to transform data
│   ├── load/                   # code to load transformed data to a postgres database
│       ├── utils.py            
│       └── transform_data.py   # Functions to load data
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

## Current DB Structure
![data_retriever_v3](https://github.com/vnbl/data_retriever/assets/21232496/12fa07db-cf99-4fd2-a578-672e52498e09)

# Current Flow Diagram
Available for editing [here](https://lucid.app/lucidchart/9458c9c0-a69d-435e-8e2a-c308dd53ffb3/edit?viewport_loc=-1888%2C-751%2C3755%2C1602%2C0_0&invitationId=inv_9832203a-f57d-4c0e-8088-e32f409d3b45) 

![data_retriever_flow](https://github.com/vnbl/data_retriever/assets/21232496/6eb560c1-e2f5-4c2b-86d6-0563c217f280)

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

AIRNOW_API_KEY='your_secret_airnow_api_key'
```

## Contributing

Outline guidelines for contributing to the project, such as how to report issues, suggest improvements, or submit pull requests.

## License

Specify the license under which the project is distributed.
