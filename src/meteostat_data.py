from datetime import datetime, timedelta
from pytz import timezone
from meteostat import Point, Hourly
import pandas as pd
import numpy as np
from src.models import MeteostatData, StationReadings
from src.database import create_postgres_session, create_postgres
from sqlalchemy import func
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_to_local_time(time_utc):
    local_time = timezone('America/Asuncion')
    utc_minus_0 = timezone('UTC')
    time_utc = utc_minus_0.localize(time_utc)
    time_utc_local = time_utc.astimezone(local_time)
    return time_utc_local.replace(tzinfo=None)

def convert_to_utc(time_local):
    local_time = timezone('America/Asuncion')
    time_localized = local_time.localize(time_local)
    utc_time = timezone('UTC')
    time_local_utc = time_localized.astimezone(utc_time)
    return time_local_utc.replace(tzinfo=None)

def fetch_meteostat_data(start, end):
    asuncion = Point(-25.2667, -57.6333, 101)
    data = Hourly(asuncion, start, end).fetch()
    return data

def transform_meteostat_data(data):
    data.index = data.index.map(convert_to_local_time)
    meteo_features = ['temp', 'rhum', 'pres', 'wspd', 'wdir']
    data = data[meteo_features]
    data.rename(columns={
        'temp': 'temperature',
        'rhum': 'humidity',
        'pres': 'pressure',
        'wspd': 'wind_speed',
        'wdir': 'wind_dir'
    }, inplace=True)
    
    data['wind_dir_cos'] = np.cos(2 * np.pi * data.wind_dir / 360)
    data['wind_dir_sin'] = np.sin(2 * np.pi * data.wind_dir / 360)
    data.drop('wind_dir', axis=1, inplace=True)
    data['date'] = data.index
    return data.round(2)

def get_meteostat_data(start, end):
    data = fetch_meteostat_data(start, end)
    return transform_meteostat_data(data)

def get_last_station_readings_timestamp(session):
    return session.query(func.max(StationReadings.date)).scalar()

def get_last_meteostat_timestamp(session):
    return session.query(func.max(MeteostatData.date)).scalar()

def determine_time_range(session):
    if session.query(MeteostatData).count() == 0:
        last_stationreadings_timestamp = get_last_station_readings_timestamp(session)
        start_utc = datetime(2019, 1, 1, 0, 0, 0, 0)
        end_utc = convert_to_utc(last_stationreadings_timestamp)
    else:
        last_meteostat_timestamp = get_last_meteostat_timestamp(session)
        start_utc = convert_to_utc(last_meteostat_timestamp + timedelta(hours=1))
        end_utc = datetime.now(timezone('UTC')).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    
    return start_utc, end_utc

def fill_meteostat_data():
    try:
        postgres_engine = create_postgres()
        with create_postgres_session(postgres_engine) as session:
            start_utc, end_utc = determine_time_range(session)
            
            if start_utc < end_utc:
                meteostat_df = get_meteostat_data(start=start_utc, end=end_utc)
                meteostat_df.to_sql('meteostat_data', session.connection(), if_exists='append', index=False)
                session.commit()
                return True
            else:
                logging.info('No new meteostat data to insert')
                return False
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False


