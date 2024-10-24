# data_retriever
 * ETL for FIUNA, Meteostat and Airnow
 * Feature engineering for inference
 * Inference
 
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

## Current DB Structure
![data_retriever_v4(1)](https://github.com/user-attachments/assets/f938c756-7d82-4f90-9ce4-19b47fa8a64b)

# Current Flow Diagram 
Available for editing [here](https://lucid.app/lucidchart/9458c9c0-a69d-435e-8e2a-c308dd53ffb3/edit?viewport_loc=-1888%2C-751%2C3755%2C1602%2C0_0&invitationId=inv_9832203a-f57d-4c0e-8088-e32f409d3b45) 

![data_retriever_flow](https://github.com/vnbl/data_retriever/assets/21232496/6eb560c1-e2f5-4c2b-86d6-0563c217f280)

## Installation

Provide instructions on how to install and set up the project, including any dependencies or prerequisites.

## Usage

Explain how to use the project, including any command-line interfaces or APIs available.
in main folder:

### Docker
```
docker build -t respira_mage .
docker run -d -p 6789:6789 respira_mage

```

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

