# `data_retriever`

## Overview

The `data_retriever` project is an end-to-end ETL (Extract, Transform, Load) pipeline for air quality forecasting in Asunción, Paraguay. It processes data from various sources such as FIUNA air pollution sensors, Meteostat weather data, and the AirNow API for sensor calibration. The processed data is used to predict air quality through machine learning models (LightGBM), and forecasts are delivered to users via Twitter and Telegram bots.

### Key Features

- **ETL for Air Quality Data**: 
  - *Bronze Layer*: Data retrieval from sensors and APIs.
  - *Silver Layer*: Data cleaning and validation.
  - *Gold Layer*: Data unification, calibration, and feature engineering for modeling.

- **Inference**:
  - Machine learning models built with Darts and LightGBM to predict air quality.

- **Bots**:
  - Daily air quality forecasts via Twitter and Telegram bots.

- **Calibration**:
  - Monthly calibration using the US Embassy’s sensor in Asunción via the AirNow API.

## Table of Contents

1. [Introduction](#introduction)
2. [Project Structure](#project-structure)
3. [Current Database Structure](#current-database-structure)
4. [Usage](#usage)
   - [Docker Setup](#docker-setup)
   - [Example .env File](#example-env-file)
5. [Contributing](#contributing)
6. [License](#license)

## Introduction

The `data_retriever` project processes air quality data, provides daily forecasts through social media, and ensures sensor calibration using data from multiple sources. It follows an ETL process and utilizes machine learning for accurate forecasting.

### Core Processes

- **ETL for Air Pollution Data**: 
  - Retrieves, cleans, and transforms raw data into a structured format for analysis.
  
- **Inference**: 
  - Utilizes historical data to predict air quality levels with LightGBM models.

- **Bots**:
  - Delivers air quality forecasts through automated social media bots.

- **Calibration**:
  - Uses AirNow data to calibrate local pollution sensors.

## Project Structure

The `data_retriever` project is divided into the following components:

- **ETL Pipeline**: Handles data collection, cleaning, and transformation.
- **Inference System**: Predicts air quality based on past data using machine learning.
- **Bots**: Provides real-time air quality updates to users via Twitter and Telegram.
- **Calibration**: Ensures sensor accuracy through regular calibration with AirNow API data.

![image](https://github.com/user-attachments/assets/9dd5240c-b033-42b7-900b-9098ecb9690c)

## Current Database Structure

The system follows a Medallion Architecture, organized into three layers:

- **Bronze Layer**: Raw data from sensors and external APIs.
- **Silver Layer**: Cleaned and validated data ready for analysis.
- **Gold Layer**: Feature-engineered data used for training models and making predictions.

![data_retriever_v4](https://github.com/user-attachments/assets/f938c756-7d82-4f90-9ce4-19b47fa8a64b)

## Usage

### Docker Setup

To quickly set up and run the `data_retriever` project in an isolated environment, use Docker.

- **Build the Docker image**:
 ```
  docker build -t <container-name> .
 ```
- **Run the Docker container**:
 ```
  docker run -d -p 6789:6789 <container-name>
 ```
This will map port 6789 from the container to your local machine.

### Example .env File

Before running the project, configure environment variables in a .env file. Rename .env.example to .env and populate with your credentials.

Example configuration:
```
# PostgreSQL Configuration
POSTGRES_USER='<your-postgres-user>'
POSTGRES_PASSWORD='<your-postgres-pass>'
POSTGRES_HOST=<your-postgres-host>'
POSTGRES_DATABASE='<your-postgres-db>'

# MySQL Configuration (for remote database)
MYSQL_USER='<your-mysql-user>'
MYSQL_PASSWORD='<your-mysql-pass>'
MYSQL_HOST='<your-mysql-host>'
MYSQL_DATABASE='<your-mysql-db>'

# Mage.ai pipeline configuration
MAGE_DATA_DIR= 'path/in/container' 
LOCAL_MAGE_DATA_DIR='local/path'
PIPELINE_HOST='localhost'
PIPELINE_PORT='6789'
```
### Contributing

We welcome contributions! To contribute, follow these steps:

- Fork the repository.
- Create a new branch:
```
git checkout -b feature-branch
```
- Make your changes and commit:
```
git commit -am 'Add new feature'
```
- Push your changes:
```
 git push origin feature-branch
```
- Open a pull request with a description of your changes.

You may find more details on each pipeline [reading these docs](https://github.com/ProyectoRespira/data_retriever/blob/documentation/etl-pipeline/README.md).

### License

This project is licensed under the AGPL (Affero General Public License). See the LICENSE file for more details.

