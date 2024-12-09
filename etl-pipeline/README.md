# Proyecto Respira Pipelines

The project consists of a total of 17 data pipelines, grouped as follows:
- **@once**: These processes run only once at the start of the project.
- **@hourly**: These processes must run once every hour.
- **@monthly**: These processes must run once a month.
- **@daily**: These processes must run once a day.

## @once: Project Initialization

- `initialize_database`: Responsible for creating and initializing the essential tables for the proper functioning of the air quality monitoring and prediction system. This process creates a total of 16 tables in the PostgreSQL database, described as follows:

  - Table `regions`: Stores information about monitored regions, such as name, code, and related data.
    - By default, data for the Gran Asunción region is stored. To edit or add other regions, you can modify the [`create_table_regions.sql`](https://github.com/ProyectoRespira/data_retriever/blob/documentation/etl-pipeline/custom/create_table_regions.sql) block.
  - Table `stations`: Contains data on monitoring and calibration stations, including their location and status.
    - By default, data from the FIUNA Monitoring Network and the U.S. Embassy monitoring station in Paraguay is stored. To edit or add other stations, you can modify the [`create_table_stations.sql`](https://github.com/ProyectoRespira/data_retriever/blob/documentation/etl-pipeline/custom/create_table_stations.sql) block.
  - Table `weather_stations`: Stores information about associated weather stations.
    - By default, data from the meteorological station at Silvio Petirossi Airport in Asunción is stored. To edit or add other stations, you can modify the [`create_table_weather_stations`](https://github.com/ProyectoRespira/data_retriever/blob/documentation/etl-pipeline/custom/create_table_weather_stations.sql) block.
  - Table `station_readings_bronze`: Records raw, unprocessed data from air quality monitoring stations.
  - Table `station_readings_silver`: Contains clean and interpolated monitoring station data for analysis.
  - Table `station_readings_gold`: Includes calibrated and aggregated data for advanced air quality analysis and reporting.
  - Table `airnow_readings_bronze`: Stores raw, unprocessed readings from pattern stations.
  - Table `airnow_readings_silver`: Contains clean and interpolated readings from pattern stations.
  - Table `weather_readings_bronze`: Stores raw, unprocessed readings from weather stations.
  - Table `weather_readings_silver`: Contains clean and interpolated readings from weather stations.
  - Table `weather_readings_gold`: Aggregates and transforms weather data for advanced analysis.
  - Table `region_readings`: Computes aggregated metrics at the regional level, such as air quality averages and deviations.
  - Table `calibration_factors`: Records calibration factors used to adjust station readings.
  - Table `inference_runs`: Stores metadata of prediction model runs.
  - Table `inference_results`: Contains prediction results, including forecasts and derived metrics.
  - Table `health_checks`: Tracks the status of stations, determining if they are active based on recent readings.

Each table has a specific function in the system, ensuring proper data management and processing required for air quality monitoring, analysis, and prediction.

![data_retriever_v4(3)](https://github.com/user-attachments/assets/e1203340-85c2-4a4b-b12d-3348f5e00e16)

## @hourly
### Weather Station Data (@hourly)
- `etl_meteostat_bronze`: Extracts raw weather data from [Meteostat](https://dev.meteostat.net/python/#installation) stations listed in the `weather_stations` table and inserts it into the `weather_readings_bronze` table.

- `etl_meteostat_silver`:
  - Validates and interpolates raw Meteostat data and inserts it into the `weather_readings_silver` table.
  - Marks processed readings in the `weather_readings_bronze` table.

- `etl_meteostat_gold`:
  - Transforms data from the `weather_readings_silver` table for inference processes.
  - Marks processed readings in the `weather_readings_silver` table.

![data_retriever_weather](https://github.com/user-attachments/assets/ab02437d-cc7e-47ee-9bd3-0105412f2b15)
 
### Pattern Station Data (@hourly)
- `etl_airnow_bronze`: Extracts raw pollution data from pattern stations in the [Airnow](https://www.airnow.gov/) service listed in the `stations` table and inserts it into the `airnow_readings_bronze` table.

- `etl_airnow_silver`:
  - Validates and interpolates raw Airnow data and inserts it into the `airnow_readings_silver` table.
  - Marks processed readings in the `airnow_readings_bronze` table.

- `etl_airnow_gold`:
  - Transforms data from the `airnow_readings_silver` table for inference processes and inserts it into the `station_readings_gold` table.
  - Marks processed readings in the `airnow_readings_silver` table.
 
![data_retriever_airnow](https://github.com/user-attachments/assets/18c846ed-25d3-41ec-95b4-aab3066fb7a1)

 
### Monitoring Station Data (@hourly)
- `etl_fiuna_bronze`:
  - Extracts raw data from the database of the [Particulate Matter Monitoring Network MP2.5 and MP10 at the Faculty of Engineering, National University of Asunción](https://www.ing.una.py/?page_id=45577) and inserts it into the `station_readings_bronze` table.
  - **Note:** To extract raw data from other monitoring stations, a new pipeline adapted to the new data source must be created.

- `etl_fiuna_silver`:
  - Validates and interpolates raw sensor data.
  - Marks processed readings in the `station_readings_bronze` table.
  - **Note:** To process raw data from other monitoring stations, a new pipeline adapted to the new data source must be created and transformed to match the `station_readings_silver` table structure.
  
- `etl_fiuna_gold_measurements`:
  - Changes the frequency of station measurements from 5 minutes to 1 hour by averaging data in hourly intervals.
  - Calibrates particulate matter measurements using calibration factors and humidity-based adjustments to improve accuracy.
  - Marks processed readings in the `station_readings_silver` table.

- `etl_fiuna_gold_aqi_stats`:
  - Calculates the Air Quality Index for each monitoring station along with various statistical variables needed for predictions, storing the results in the `station_readings_gold` table.
  
- `etl_fiuna_regional_stats`:
  - Transforms data in the `station_readings_gold` table to compute regional averages and other statistical values required for predictions.
 
![data_retriever_fiuna](https://github.com/user-attachments/assets/b0c05ec8-bab4-4713-be39-aeacc4468007)


### Inference Pipeline
- `inference_results`:
  - Records metadata for the inference process in the `inference_runs` table.
  - Extracts data from `station_readings_gold`, `region_readings`, and `weather_readings_gold` tables.
  - Loads prediction models and performs inference for each monitoring station.
  - Saves the results in the `inference_results` table.
  
![data_retriever_inference](https://github.com/user-attachments/assets/e21655ae-9318-434c-9fb8-2e403e06d08d)

## @monthly
### Remote Calibration 
The system calculates calibration factors for monitoring stations in the same region using data from a **pattern station** and relative humidity data from **weather stations**.
- `calibration_factors`:
  - Extracts the last 3 months of pollution data from each **monitoring station** (`station_readings_silver`), its corresponding **pattern station** (`station_readings_gold`), and humidity data from its corresponding **weather station** (`weather_readings_gold`).
  - Adjusts humidity readings based on the strategy proposed by [Crilley et al.](https://amt.copernicus.org/articles/11/709/2018/).
  - Calculates the signal amplification factor for monitoring station sensors and inserts it into the `calibration_factors` table.
  - This factor is used in the `etl_fiuna_gold_measurements` process to correct monitoring station readings.

![data_retriever_calibration](https://github.com/user-attachments/assets/4e540bed-578d-40e0-9d9b-6ef4c51e9703)

## @daily: Bots
The bot design is based on the [linkaBot](https://github.com/melizeche/linkaBot/tree/main) project for the [AireLibre](https://github.com/melizeche/AireLibre) project.

- `telegram_bot` and `twitter_bot`:
  - Extracts metadata from the latest inference recorded in `inference_results`.
  - Calculates regional averages of predictions.
  - Constructs and sends daily messages via Telegram and X (formerly Twitter).

