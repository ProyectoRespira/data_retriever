from sqlalchemy import func, distinct, and_
from datetime import datetime, timedelta
from src.models import StationsReadingsRaw, StationReadings, ExternalData
from src.database import create_postgres, create_postgres_session
import pandas as pd
import numpy as np
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def apply_correction_factor(df, station_id, pm, humidity):
    correction_factors = {
        1: 3.9477741393724357,
        2: 5.773891499911656,
        3: 2.2191051063687777,
        4: 3.8382500135208697,
        5: 39.44025115478353,
        7: 9.414312741845814,
        10: 8.133051717401786
    }
    correction_factor = correction_factors.get(station_id, 1.0)

    df_filtered = df[df['hour'] >= datetime(2024, 1, 1)]
    if not df_filtered.empty:
        df_merged = pd.merge(df_filtered, humidity, how='inner', left_on='hour', right_on='date')
        df_merged['C_RH'] = df_merged['humidity'].apply(lambda x: 1 if x < 65 else (0.0121212*x) + 1 if x < 85 else (1 + ((0.2/1.65)/(-1 + 100/min(x, 90)))))
        df_merged.bfill(inplace=True)
        df_merged[pm] = df_merged[pm] / df_merged['C_RH']
        df_merged[pm] *= correction_factor
        df_merged.dropna(axis=0, how='any')
    return df_merged[pm]

def validate_date_hour(date, hour):
    date_pattern = re.compile(r'^\d{1,2}-\d{1,2}-\d{4}$')
    hour_pattern = re.compile(r'^\d{1,2}:\d{2}$')

    if not date_pattern.match(date):
        return False
    if not hour_pattern.match(hour):
        return False
    try:
        date_obj = datetime.strptime(date, '%d-%m-%Y')
        if date_obj.year < 2019:
            return False
    except ValueError:
        return False
    
    return True

def get_last_transformation_timestamp(session, station_id):
    return session.query(func.max(StationReadings.date)).filter_by(station=station_id).scalar()

def fetch_raw_readings(session, station_id, last_transformation_timestamp):
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

def transform_raw_readings_to_df(raw_readings):
    df = pd.DataFrame([vars(reading) for reading in raw_readings])
    df['fecha_hora'] = df['fecha'] + ' ' + df['hora']
    df['datetime'] = pd.to_datetime(df['fecha_hora'], format='%d-%m-%Y %H:%M', errors='coerce')
    df = df.dropna()
    numeric_columns = ['mp1', 'mp2_5', 'mp10', 'temperatura', 'humedad', 'presion']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
    df.fillna(value=-144.2, inplace=True)
    df['hour'] = df['datetime'].dt.floor('h')
    return df.groupby('hour').agg({
        'mp1': 'mean',
        'mp2_5': 'mean',
        'mp10': 'mean',
        'temperatura': 'mean',
        'humedad': 'mean',
        'presion': 'mean'
    }).reset_index()


def fetch_meteostat_humidity(session, last_transformation_timestamp):
    meteostat_humidity = session.query(ExternalData.date, ExternalData.humidity).filter(
        ExternalData.date > last_transformation_timestamp
    ).all()
    return pd.DataFrame([{'date': reading.date, 'humidity': reading.humidity} for reading in meteostat_humidity])

def update_or_insert_station_readings(session, station_id, hourly_readings):
    for _, row in hourly_readings.iterrows():
        existing_reading = session.query(StationReadings).filter_by(station=station_id, date=row['hour']).first()
        if existing_reading:
            existing_reading.pm1 = row['mp1']
            existing_reading.pm2_5 = row['mp2_5']
            existing_reading.pm10 = row['mp10']
            existing_reading.temperature = row['temperatura']
            existing_reading.humidity = row['humedad']
            existing_reading.pressure = row['presion']
            existing_reading.aqi_pm2_5 = None
            existing_reading.aqi_pm10 = None
        else:
            reading = StationReadings(
                station=station_id,
                date=row['hour'],
                pm1=row['mp1'],
                pm2_5=row['mp2_5'],
                pm10=row['mp10'],
                temperature=row['temperatura'],
                humidity=row['humedad'],
                pressure=row['presion'],
                aqi_pm2_5=None,
                aqi_pm10=None
            )
            session.add(reading)
    session.commit()

def get_aqi_25(x):
    if x <= 12:
        return round(x * 50 / 12, 0)
    elif x <= 35.4:
        return round(51 + (x - 12.1) * 49 / 23.3, 0)
    elif x <= 55.4:
        return round(101 + (x - 35.5) * 49 / 19.9, 0)
    elif x <= 150.4:
        return round(151 + (x - 55.5) * 49 / 94.4, 0)
    elif x <= 250.4:
        return round(201 + (x - 150.5) * 99 / 99.9, 0)
    elif x <= 350.4:
        return round(301 + (x - 250.5) * 99 / 99.9, 0)
    else:
        return round(401 + (x - 350.5) * 99 / 149.9, 0)

def get_aqi_10(x):
    if x <= 54:
        return round(x * 50 / 54, 0)
    elif x <= 154:
        return round(51 + (x - 55) * 49 / 99, 0)
    elif x <= 254:
        return round(101 + (x - 155) * 49 / 99, 0)
    elif x <= 354:
        return round(151 + (x - 255) * 49 / 99, 0)
    elif x <= 424:
        return round(201 + (x - 355) * 99 / 69, 0)
    elif x <= 504:
        return round(301 + (x - 425) * 99 / 79, 0)
    else:
        return round(401 + (x - 504) * 99 / 100, 0)

def calculate_aqi_for_station(session, station_id):
    raw_readings = session.query(StationReadings).filter(
        StationReadings.station == station_id,
        (StationReadings.aqi_pm2_5 == None) | (StationReadings.aqi_pm10 == None)
    ).all()

    for reading in raw_readings:
        pm2_5_24h_mean = session.query(func.avg(StationReadings.pm2_5)).filter(
            StationReadings.station == station_id,
            StationReadings.date >= reading.date - timedelta(hours=24),
            StationReadings.date <= reading.date
        ).scalar()

        pm10_24h_mean = session.query(func.avg(StationReadings.pm10)).filter(
            StationReadings.station == station_id,
            StationReadings.date >= reading.date - timedelta(hours=24),
            StationReadings.date <= reading.date
        ).scalar()

        if pm2_5_24h_mean is not None and pm10_24h_mean is not None:
            reading.aqi_pm2_5 = get_aqi_25(pm2_5_24h_mean)
            reading.aqi_pm10 = get_aqi_10(pm10_24h_mean)

    session.commit()

def transform_raw_data(session):
    station_ids = session.query(distinct(StationsReadingsRaw.station_id)).all()
    
    for station_id in station_ids:
        station_id = station_id[0]
        last_transformation_timestamp = get_last_transformation_timestamp(session, station_id)
        if last_transformation_timestamp is None:
            last_transformation_timestamp = datetime(2024, 1, 1, 0, 0, 0, 0)

        raw_readings = fetch_raw_readings(session, station_id, last_transformation_timestamp)
        if not raw_readings:
            logging.info(f'No new data for station {station_id}')
            continue

        hourly_readings = transform_raw_readings_to_df(raw_readings)
        meteostat_humidity = fetch_meteostat_humidity(session, last_transformation_timestamp)

        if not meteostat_humidity.empty:
            hourly_readings['mp2_5'] = apply_correction_factor(hourly_readings, station_id, 'mp2_5', meteostat_humidity)
            hourly_readings['mp10'] = apply_correction_factor(hourly_readings, station_id, 'mp10', meteostat_humidity)

        hourly_readings = hourly_readings.round({'mp1': 2, 'mp2_5': 2, 'mp10': 2, 'temperatura': 2, 'humedad': 2, 'presion': 2})
        hourly_readings.ffill(inplace=True)
        update_or_insert_station_readings(session, station_id, hourly_readings)
        logging.info(f'Processed data for station {station_id}')

def calculate_aqi(session):
    stations = session.query(distinct(StationReadings.station)).all()
    for station_id in stations:
        calculate_aqi_for_station(session, station_id[0])
        logging.info(f'Calculated AQI for station {station_id[0]}')

def fill_station_readings():
    try:
        postgres_engine = create_postgres()
        with create_postgres_session(postgres_engine) as postgres_session:
            transform_raw_data(postgres_session)
            calculate_aqi(postgres_session)
            return True
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False
    finally:
        postgres_session.close()
