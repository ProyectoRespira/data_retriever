from sqlalchemy import func, distinct, and_
from src.models import StationReadings, Regions, StationsReadingsRaw, WeatherReadings, CalibrationFactors, WeatherStations
from src.time_utils import validate_date_hour
from src.querys import query_last_stationreadings_timestamp, fetch_pattern_station_id_region
import pandas as pd

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_raw_readings_for_feature_transformation(session, station_id, last_transformation_timestamp):
    try:
        raw_readings = session.query(
            StationsReadingsRaw.fecha,
            StationsReadingsRaw.hora,
            StationsReadingsRaw.mp2_5,
            StationsReadingsRaw.mp10,
            StationsReadingsRaw.mp1,
            StationsReadingsRaw.temperatura,
            StationsReadingsRaw.humedad,
            StationsReadingsRaw.presion
            ).filter(
            StationsReadingsRaw.station_id == station_id
        )

        valid_readings = [
            reading for reading in raw_readings
            if validate_date_hour(reading.fecha, reading.hora)
        ]

        valid_ids = [reading.id for reading in valid_readings]

        return session.query(StationsReadingsRaw).filter(
            and_(
                StationsReadingsRaw.id.in_(valid_ids),
                func.to_timestamp(func.concat(StationsReadingsRaw.fecha, ' ', StationsReadingsRaw.hora), 'DD-MM-YYYY HH24:MI') > last_transformation_timestamp,
            )
        ).all()
    
    except Exception as e:
        logging.info(f'An error occurred while fetching raw readings: {e}')
        return None
    
def convert_readings_to_dataframe(raw_readings):
    return pd.DataFrame([vars(reading) for reading in raw_readings])

def add_date_column_as_index(df):
    df['date'] = df['fecha'] + ' ' + df['hora']
    df['date'] = pd.to_datetime(df['date'], format = '%d-%m-%Y %H:%M', errors = 'coerce')
    df.set_index('date', inplace = True)
    return df.drop(columns = ['fecha', 'hora'], axis = 1)
    
def rename_columns(df, renaming_dict):
    # renaming_dict = {'mp1': 'pm1',
    #                 'mp2_5': 'pm2_5',
    #                 'mp10': 'pm10',
    #                 'temperatura': 'temperature',
    #                 'humedad': 'humidity',
    #                 'presion': 'pressure'}

    return df.rename(columns = renaming_dict)

def change_dtype_to_numeric(df, numeric_columns):
    #numeric_columns = ['pm1', 'pm2_5', 'pm10', 'temperature', 'humidity', 'pressure']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
    return df

def change_raw_readings_frequency(df):
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError('The Dataframe index must be a DatetimeIndex')

    df.index = df.index.floor('h')
    return df.groupby(df.index).agg({
        'pm1': 'mean',
        'pm2_5': 'mean',
        'pm10': 'mean',
        'temperature': 'mean',
        'humidity': 'mean',
        'pressure': 'mean'
    }).reset_index()

def get_calibration_timerange(df):
    start = df.index.min()
    end = df.index.max()
    return start, end

def get_humidity_dataframe_from_weather_readings(session, last_transformation_timestamp, station_id):
    _, region = fetch_pattern_station_id_region(session, station_id)

    humidity_readings = session.query(
        WeatherReadings.date, 
        WeatherReadings.humidity
        ).join(
            WeatherStations, WeatherReadings.weather_station == WeatherStations.id
        ).join(
            Regions, WeatherStations.region == Regions.region_code
        ).filter(
            Regions.region_code == region,
            WeatherReadings.date > last_transformation_timestamp,
    ).all()

    humidity_df = pd.DataFrame([{'date': reading.date, 'region_humidity' : reading.humidity} for reading in humidity_readings])
    humidity_df.set_index('date', inplace = True)

    return humidity_df

def apply_calibration_factor(session, df, station_id, pm_columns, humidity_df):
    pass

def transform_raw_readings_to_station_readings(session, station_id):
    # setup
    renaming_dict = {'mp1': 'pm1',
                    'mp2_5': 'pm2_5',
                    'mp10': 'pm10',
                    'temperatura': 'temperature',
                    'humedad': 'humidity',
                    'presion': 'pressure'}

    numeric_columns = ['pm1', 'pm2_5', 'pm10', 'temperature', 'humidity', 'pressure']

    last_transformation_timestamp = query_last_stationreadings_timestamp(session, station_id)
    raw_station_readings = get_raw_readings_for_feature_transformation(session, station_id, last_transformation_timestamp)
    
    # basic transformations
    df = convert_readings_to_dataframe(raw_station_readings)
    df = add_date_column_as_index(df)
    df = rename_columns(df, renaming_dict)
    df = change_dtype_to_numeric(df, numeric_columns)
    df = change_raw_readings_frequency(df)

    # pm calibration
    pm_columns = ['pm1', 'pm2_5', 'pm10'] # parameters to calibrate

    humidity_df = get_humidity_dataframe_from_weather_readings(session, last_transformation_timestamp, station_id)
    df = apply_calibration_factor(session, df, station_id, pm_columns, humidity_df)






    









