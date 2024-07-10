from src.models import StationReadings, CalibrationFactors, StationsReadingsRaw, Stations
from datetime import timedelta
from sqlalchemy import func, cast, Float, and_, Time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_pattern_station_id_region(session, station_id):
    '''
    returns pattern_station_id and region for a specific station_id
    '''
    region = session.query(Stations.region).filter(
        Stations.id == station_id
    ).scalar()

    pattern_station_id = session.query(Stations.id).filter(
        Stations.is_pattern_station == True,
        Stations.region == region
    )
    return pattern_station_id, region

def humidity_correction(df):
    df['C_RH'] = df['humidity'].apply(lambda x: 1 if x < 65 else (0.0121212*x) + 1 if x < 85 else (1 + ((0.2/1.65)/(-1 + 100/min(x, 90)))))
    pass

def get_station_average_corrected_for_humidity(station_id, start, end):
    pass

def get_pattern_station_average(pattern_station_id, start, end):
    pass

def get_calibration_date_setup(month_year, station_id):
    pass

def calcuate_calibration_factor(month_year, station_id):

    pattern_station_id, region = get_pattern_station_id_region(station_id)

    start_cal, end_cal, start_usage, end_usage = get_calibration_date_setup(month_year, station_id)
    
    station_mean = get_station_average_corrected_for_humidity(station_id, start_cal, end_cal)

    pattern_mean = get_pattern_station_average(pattern_station_id, start_cal, end_cal)

    calibration_factor = pattern_mean/station_mean

    calibration_info = {'region': region,
                        'station_id': station_id,
                        'date_start_cal': start_cal,
                        'date_end_cal': end_cal,
                        'station_mean': station_mean, 
                        'pattern_mean': pattern_mean,
                        'date_start': start_usage,
                        'date_end': end_usage,
                        'calibration_factor': calibration_factor}

    return calibration_info

def load_calibration_factor(session, calibration_info):
    
    calibration = CalibrationFactors(
        region = calibration_info['region'],
        station_id = calibration_info['station_id'],
        date_start_cal = calibration_info['date_start_cal'],
        date_end_cal = calibration_info['date_end_cal'],
        station_mean = calibration_info['station_mean'],
        pattern_mean = calibration_info['pattern_mean'],
        date_start = calibration_info['date_start'],
        date_end = calibration_info['date_end'],
        calibration_factor = calibration_info['calibration_factor']
    )

    session.add(calibration)
    session.commit()

    logging.info(f'Calibration factor for station {calibration_info['station_id']} 
                 for period {calibration_info['date_start']}/{calibration_info['date_end']} inserted correctly')
    
    return True


    