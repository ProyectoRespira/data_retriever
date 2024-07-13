from src.initialize_db import create_postgres_tables
from src.extract.extract_data import extract_fiuna_data, extract_meteostat_data, extract_airnow_data
from src.transform.transform_data import transform_fiuna_data, transform_meteostat_data, transform_airnow_data
from src.load.load_data import load_station_readings_raw, load_weather_data, load_airnow_data
from src.calibration_factors.calibration_factors import insert_calibration_factor, backfill_calibration_factors
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    create_postgres_tables()
    
    # fiuna_data, extract_status = extract_fiuna_data()
    # if extract_status is False:
    #     return 'Error: Extracting data from FIUNA failed'
    
    # transformed_fiuna_data, transform_status = transform_fiuna_data(fiuna_data)
    # if transform_status is False:
    #     return 'Error: Transforming data from FIUNA failed'
    
    # load_status = load_station_readings_raw(transformed_fiuna_data)
    # if load_status is False: 
    #     return 'Error: Loading data to StationReadingsRaw failed'
    
    # logging.info('Success: Data from FIUNA loaded correctly')
    
    # # Meteostat Data
    # meteostat_data, extract_status = extract_meteostat_data()
    # if extract_status is False:
    #     return 'Error: Extracting data from Meteostat failed'
    
    # transformed_meteostat_data, transform_status = transform_meteostat_data(meteostat_data)
    # if transform_status is False:
    #     return 'Error: Transforming data from Meteostat failed'
    
    # load_status = load_weather_data(transformed_meteostat_data)
    # if load_status is False:
    #     return 'Error: Loading data to WeatherData failed'
    
    # logging.info('Success: Data from Meteostat loaded correctly')

    # airnow data
    airnow_data, extract_status = extract_airnow_data()
    if extract_status is False:
        return 'Error: Extracting data from AirNow failed.'
    
    transformed_airnow_data, transform_status = transform_airnow_data(airnow_data)
    
    if transform_status is False:
        return 'Error: Transforming data from AirNow failed.'
    
    load_status = load_airnow_data(transformed_airnow_data)
    if load_status is False:
        return 'Error: Loading data to USAirQualityReadings failed.'
    
    # calibration factors
    # period = datetime(2024, 1, 1, 0, 0, 0)
    # station_id = 1
    # calibration_status = insert_calibration_factor(period, station_id)
    calibration_status = backfill_calibration_factors()
    if calibration_status is False:
        return 'Calibration calculation failed'

    return 'Process finished correctly'


    

if __name__ == "__main__":
    message = main()
    logging.info(message)