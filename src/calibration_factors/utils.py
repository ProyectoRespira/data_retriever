from src.models import StationReadings, CalibrationFactors, StationsReadingsRaw, Stations, WeatherReadings, WeatherStations, Regions
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import func, cast, Float, and_, Time
import pandas as pd
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

def get_station_average_corrected_for_humidity(session, station_id, start, end):
    pm_readings = session.query(StationsReadingsRaw.mp2_5).filter(
        StationsReadingsRaw.station_id == station_id,
        and_(
            func.to_date(StationsReadingsRaw.fecha, 'DD-MM-YYYY') + func.cast(StationsReadingsRaw.hora, Time) >= start,
                func.to_date(StationsReadingsRaw.fecha, 'DD-MM-YYYY') + func.cast(StationsReadingsRaw.hora, Time) <= end,
                StationsReadingsRaw.mp2_5.isnot(None)
        )
    ).scalar()

    _, region = get_pattern_station_id_region(session, station_id)

    humidity_readings = session.query(WeatherReadings.humidity).join(
        WeatherStations, WeatherReadings.weather_station == WeatherStations.id
    ).join(
        Regions, WeatherStations.region == Regions.region_code
    ).filter(
        Regions.region_code == region,
        WeatherReadings.date >= start,
        WeatherReadings.date <= end
    ).all()

    # convert pm_readings and humidity_readings to a df, merge both dfs. Note: pm_readings has a 5 minute frequency, humidity_readings 
    # has a 1 hour frequency. The merged df should have 3 columns: date, pm and humidity. 
    # Frequency should be 5 minutes, use bbfill to fill missing humidity readings.

    pm_df = pd.DataFrame(pm_readings, columns=['fecha', 'hora', 'mp2_5'])
    pm_df['datetime'] = pd.to_datetime(pm_df['fecha'] + ' ' + pm_df['hora'].astype(str), format = '%d-%m-%Y %H:%M')
    pm_df = pm_df.set_index('datetime').drop(columns=['fecha', 'hora'])

    humidity_df = pd.DataFrame(humidity_readings, columns=['date', 'humidity'])
    humidity_df['C_RH'] = humidity_df['humidity'].apply(lambda x: 1 if x < 65 else (0.0121212*x) + 1 if x < 85 else (1 + ((0.2/1.65)/(-1 + 100/min(x, 90)))))
    humidity_df = humidity_df.set_index('date').drop(columns=['humidity'])

    merged_df = pm_df.join(humidity_df.resample('5T').ffill(), how='left')

    merged_df['pm2_5_corrected'] = merged_df['mp2_5']/merged_df['C_RH']

    corrected_average_pm2_5 = merged_df['pm2_5_corrected'].mean()

    return corrected_average_pm2_5
    


def get_pattern_station_average(session, pattern_station_id, start, end):
    pattern_readings = session.query(StationReadings.pm2_5).filter(
        StationReadings.id == pattern_station_id,
        StationReadings.date >= start,
        StationReadings.date <= end
    ).all()

    if pattern_readings:
        pattern_average = sum([reading.pm2_5 for reading in pattern_readings]) / len(pattern_readings)
    else:
        pattern_average = None

    return pattern_average


def get_calibration_date_setup(month_year):
    # validate that month_year is in the correct format
    start_cal = month_year - relativedelta(months=3)
    end_cal = month_year
    start_usage = month_year + timedelta(hours=1)
    end_usage = start_usage + relativedelta(months=1)

    return start_cal, end_cal, start_usage, end_usage

def calcuate_calibration_factor(session, month_year, station_id):

    pattern_station_id, region = get_pattern_station_id_region(session, station_id)

    start_cal, end_cal, start_usage, end_usage = get_calibration_date_setup(month_year)
    
    station_mean = get_station_average_corrected_for_humidity(session, station_id, start_cal, end_cal)

    pattern_mean = get_pattern_station_average(session, pattern_station_id, start_cal, end_cal)

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

def commit_calibration_factor(session, calibration_info):
    
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

    logging.info(f"Calibration factor for station {calibration_info['station_id']} "
             f"for period {calibration_info['date_start']}/{calibration_info['date_end']} inserted correctly")
    return True


    