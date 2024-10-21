from src.load.utils import insert_station_readings_raw, insert_weather_data, insert_airnow_data
from src.database import create_postgres_session, create_postgres
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_station_readings_raw(transformed_fiuna_data):
    try:
        postgres_session = create_postgres_session(create_postgres())
        status = insert_station_readings_raw(postgres_session, transformed_fiuna_data)
        return status
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        return False

def load_weather_data(transformed_meteostat_data):
    try:
        postgres_session = create_postgres_session(create_postgres())
        if transformed_meteostat_data is not None:
            status = insert_weather_data(postgres_session, transformed_meteostat_data)
            return status
        else:
            logging.info('No new meteostat data to insert')
            return True
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        return False
    finally:
        if postgres_session:
            postgres_session.close()

def load_airnow_data(transformed_airnow_data):
    postgres_session = None
    
    try:
        postgres_session = create_postgres_session(create_postgres())
        
        if transformed_airnow_data is not None:
            status = insert_airnow_data(postgres_session, transformed_airnow_data)
            return status
        else:
            logging.info('No new airnow data to insert')
            return True
    
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        return False
    
    finally:
        if postgres_session:
            postgres_session.close()