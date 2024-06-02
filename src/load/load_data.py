from src.load.utils import insert_station_readings_raw
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
