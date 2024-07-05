
from sqlalchemy import distinct
from src.extract.utils import (
    select_new_records_from_fiuna_origin_table, 
    get_last_raw_measurement_id, 
    determine_meteostat_query_time_range, 
    get_weather_station_ids,
    fetch_meteostat_data, 
    define_airnow_api_url, 
    get_pattern_station_ids
)
from src.database import create_postgres_session, create_postgres, create_mysql
from src.models import Stations
import requests

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_fiuna_data(): # modify this method to only extract data
    logging.info('Starting retrieve_data...')
    fiuna_data = {}
    try:
        mysql_engine = create_mysql()
        postgres_engine = create_postgres()
        with create_postgres_session(postgres_engine) as postgres_session:
            station_ids = postgres_session.query(distinct(Stations.id)).filter(
                Stations.is_pattern_station == False
            ).all() 
            
            for station_id in station_ids:
                table_name = f'Estacion{station_id[0]}'
                last_measurement_id = get_last_raw_measurement_id(postgres_session, station_id[0])
                fiuna_data[station_id[0]] = select_new_records_from_fiuna_origin_table(mysql_engine, table_name, last_measurement_id)
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
    logging.info('Starting extract_meteostat_data...')
    session = None
    try:
        postgres_engine = create_postgres()
        with create_postgres_session(postgres_engine) as session:
            station_ids = get_weather_station_ids(session)
            results = {}
            for station_id in station_ids:
                start_utc, end_utc = determine_meteostat_query_time_range(session)
                if start_utc < end_utc:
                    meteostat_df = fetch_meteostat_data(session, start_utc, end_utc, station_id)
                    logging.info(f'Meteostat data for station {station_id} retrieved successfully')
                    results[station_id] = meteostat_df
                else:
                    logging.info(f'No new meteostat data to retrieve for station {station_id}')
            return results, True
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None, False
    finally:
        if session:
            session.close()

def extract_airnow_data():
    logging.info('Starting extract_airnow_data...')
    session = None
    try:
        postgres_engine = create_postgres()
        with create_postgres_session(postgres_engine) as session:
            airnow_stations_id = get_pattern_station_ids(session)
            responses = {}
            for station_id in airnow_stations_id:
                api_url = define_airnow_api_url(session, station_id)
                response = requests.get(api_url)
                if response.status_code == 200:
                    logging.info(f'Data retrieved from AirNow for station with ID = {station_id} Successfully')
                    responses[station_id] = response.json()
                else:
                    raise Exception(f'Failed to fetch data: {response.status_code}')
            return responses, True # tuple with responses and status
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        return None, False
    finally:
        if session:
            session.close()

