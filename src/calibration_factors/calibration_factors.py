from src.calibration_factors.utils import compute_calibration_factor, insert_calibration_factor_into_db
from src.database import create_postgres, create_postgres_session
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def insert_calibration_factor(period, station_id):
    try:
        postgres_engine = create_postgres()

        with create_postgres_session(postgres_engine) as session:
            calibration_info = compute_calibration_factor(session, period, station_id)
            
            if calibration_info is None:
                logging.info('No calibration factor for insertion')
                return True

            insert_calibration_factor_into_db(session, calibration_info)
            return True
    
    except Exception as e:
        logging.exception('An exception occurred during calibration factor calculation and insertion')
        return False
    
    finally:
        if postgres_engine:
            postgres_engine.dispose()
        if session:
            session.close()