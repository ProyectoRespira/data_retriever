from src.calibration_factors.utils import compute_calibration_factor, insert_calibration_factor_into_db, fetch_pattern_station_id_region, get_unique_calibration_dates
from src.database import create_postgres, create_postgres_session
from src.querys import fetch_station_ids
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


def backfill_calibration_factors():
    try:
        postgres_engine = create_postgres()

        with create_postgres_session(postgres_engine) as session:
            stations = fetch_station_ids(session)
            
            for station_id in stations:
                pattern_station_id, _ = fetch_pattern_station_id_region(session, station_id)
                calibration_dates = get_unique_calibration_dates(session, pattern_station_id)

                for cal_date in calibration_dates:
                    insert_calibration_factor(cal_date, station_id)
            return True
    
    except Exception as e:
        logging.exception('An exception occurred during backfill')
        return False
    
    finally:
        if postgres_engine:
            postgres_engine.dispose()
        if session:
            session.close()