
from sqlalchemy import distinct
from src.extract.utils import select_new_records_from_origin_table, get_last_measurement_id
from src.database import create_postgres_session, create_postgres, create_mysql
from src.models import Stations

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_fiuna_data(): # modify this method to only extract data
    logging.info('Starting retrieve_data...')
    fiuna_data = {}
    try:
        mysql_engine = create_mysql()
        postgres_engine = create_postgres()
        with create_postgres_session(postgres_engine) as postgres_session:
            station_ids = postgres_session.query(distinct(Stations.id)).all() 
            for station_id in station_ids:
                table_name = f'Estacion{station_id[0]}'
                last_measurement_id = get_last_measurement_id(postgres_session, station_id[0])
                fiuna_data[station_id[0]] = select_new_records_from_origin_table(mysql_engine, table_name, last_measurement_id)
            logging.info("Data retrieved successfully")
        return fiuna_data, True
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None, False
    finally:
        if mysql_engine:
            mysql_engine.dispose()
        if postgres_session:
            postgres_session.close()

def extract_meteostat_data():
    pass


