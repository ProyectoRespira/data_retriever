from src.features.utils import transform_raw_readings_to_station_readings, upsert_station_readings_into_db
from src.database import create_postgres, create_postgres_session
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_features():
    try:
        postgres_engine = create_postgres()

        with create_postgres_session(postgres_engine) as session:
            station_id = 3
            df = transform_raw_readings_to_station_readings(session, station_id)
            upsert_station_readings_into_db(session, df)
            return True
    
    except Exception as e:
        logging.exception('An exception occurred during feature engineering')
        return False
    
    finally:
        if postgres_engine:
            postgres_engine.dispose()
        if session:
            session.close()