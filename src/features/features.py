from src.features.utils import transform_raw_readings_to_station_readings, upsert_station_readings_into_db
from src.features.aqi import compute_and_update_aqi_for_station_readings
from src.features.stats import update_station_readings_stats
from src.database import create_postgres, create_postgres_session
from src.querys import fetch_station_ids
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_features():
    try:
        postgres_engine = create_postgres()

        with create_postgres_session(postgres_engine) as session:
            stations = fetch_station_ids(session)
            for station_id in stations:
                logging.info(f'Starting basic transformations for station {station_id}')
                
                df, message = transform_raw_readings_to_station_readings(session, station_id)
                if df is None:
                    logging.warning(message)
                    return True
                else:
                    logging.info(message)
                
                status = upsert_station_readings_into_db(session, df)
                if status is False:
                    logging.warning(f'Could not upsert station readings from station {station_id}')
                # calculate and add AQI
                status = compute_and_update_aqi_for_station_readings(session, station_id)
                if status is False:
                    logging.warning(f'Could not calculate and add AQI')
                # calculate and add stats
                status = update_station_readings_stats(session, station_id)
                if status is False:
                    logging.warning(f'Could not calculate and insert stats')
            return True
    
    except Exception as e:
        logging.exception('An exception occurred during feature engineering')
        return False
    finally:
        if postgres_engine:
            postgres_engine.dispose()
        if session:
            session.close()