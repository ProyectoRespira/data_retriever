from sqlalchemy import func, distinct, and_
from src.models import StationReadings, StationsReadingsRaw, WeatherReadings, CalibrationFactors
from src.time_utils import validate_date_hour
import pandas as pd

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_last_transformation_timestamp(session, station_id):
    return session.query(func.max(StationReadings.date)).filter_by(station=station_id).scalar()

def query_raw_readings(session, station_id, last_transformation_timestamp):
    try:
        raw_readings = session.query(StationsReadingsRaw).filter(
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
    
def change_raw_readings_frequency(raw_readings):
    df = convert_readings_to_dataframe(raw_readings)
    df['fecha_hora'] = df['fecha'] + ' ' + df['hora']
    df['datetime'] = pd.to_datetime(df['fecha_hora'], format='%d-%m-%Y %H:%M', errors='coerce')
    df = df.dropna()
    numeric_columns = ['mp1', 'mp2_5', 'mp10', 'temperatura', 'humedad', 'presion']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
    df.fillna(value=-144.2, inplace=True)
    df['date'] = df['datetime'].dt.floor('h')
    return df.groupby('date').agg({
        'mp1': 'mean',
        'mp2_5': 'mean',
        'mp10': 'mean',
        'temperatura': 'mean',
        'humedad': 'mean',
        'presion': 'mean'
    }).reset_index()

def rename_columns(df):
    return df.rename(columns = {'mp1': 'pm1',
                                'mp2_5': 'pm2_5',
                                'mp10': 'pm10',
                                'temperatura': 'temperature',
                                'humedad': 'humidity',
                                'presion': 'pressure'})

def query_weatherdata_humidity(session, last_transformation_timestamp):
    weatherdata_humidity = session.query(WeatherReadings.date, WeatherReadings.humidity).filter(
        WeatherReadings.date > last_transformation_timestamp
    ).all()
    return pd.DataFrame([{'date': reading.date, 'humidity' : reading.humidity} for reading in weatherdata_humidity])

def query_calibration_factors(session, last_transformation_timestamp, station):
    calibration_factors = session.query(
        CalibrationFactors.date, 
        CalibrationFactors.calibration_factor,
        CalibrationFactors.station).filter(
            and_(
                CalibrationFactors.date > last_transformation_timestamp, 
                CalibrationFactors.station_id == station
                )
    ).all()
    return pd.DataFrame([{'date': reading.date, 
                          'calibration_factor': reading.calibration_factor,
                          'station': reading.station_id} for reading in calibration_factors])


def apply_calibration_factor(session, df, station_id, pm, humidity):
    last_transformation_timestamp = get_last_transformation_timestamp(session, station_id)
    calibration_factors = query_calibration_factors(session, last_transformation_timestamp, station_id)
    
    df_merged = pd.merge(df, humidity, how='inner', left_on='date', right_on='date')
    df_merged = pd.merge(df_merged, calibration_factors, left_on='date', right_on='date')
    df_merged.dropna(subset=['calibration_factor'], inplace=True)
    df_merged['C_RH'] = df_merged['humidity'].apply(lambda x: 1 if x < 65 else (0.0121212*x) + 1 if x < 85 else (1 + ((0.2/1.65)/(-1 + 100/min(x, 90)))))
    df_merged.bfill(inplace=True)
    df_merged.ffill(inplace=True)
    df_merged[pm] = df_merged[pm] * df_merged['calibration_factor']
    df_merged.dropna(axis = 0, how='any')

    return df_merged







