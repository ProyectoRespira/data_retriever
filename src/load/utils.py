import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def insert_station_readings_raw(postgres_session, transformed_fiuna_data):
    logging.info('Starting insert_station_readings_raw...')
    try:
        station_ids = transformed_fiuna_data.keys()
        for station_id in station_ids: 
            postgres_session.add_all(transformed_fiuna_data[station_id])
            postgres_session.commit()
            logging.info(f'{len(transformed_fiuna_data[station_id])} records from Station {station_id} inserted.')
        return True
    except Exception as e:
        logging.exception(f"Something bad happened: {e}")
        postgres_session.rollback()
        return False


def insert_weather_data(postgres_session, transformed_meteostat_data):
    logging.info('Starting insert_weather_data...')
    try:
        transformed_meteostat_data.to_sql('weather_data', postgres_session.connection(), if_exists='append', index=False)
        postgres_session.commit()
        logging.info('Meteostat data inserted successfully')
        return True
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        postgres_session.rollback()
        return False