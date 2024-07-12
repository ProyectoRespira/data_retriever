from src.calibration_factors.utils import calcuate_calibration_factor, commit_calibration_factor
from src.database import create_postgres, create_postgres_session
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calculate_insert_calibration_factor(period, station_id):
    try:
        postgres_engine = create_postgres()

        with create_postgres_session(postgres_engine) as session:
            calibration_info = calcuate_calibration_factor(session, period, station_id)
            commit_calibration_factor(session, calibration_info)
            return True
    except Exception as e:
        logging.exception(f'An exception occurred: {e}')
        return False
    finally:
        if postgres_engine:
            postgres_engine.dispose()
        if session:
            session.close()
            