import pandas as pd
import numpy as np
from sqlalchemy import func
from datetime import datetime, timedelta
from pytz import timezone
from meteostat import Point, Hourly
from src.models import MeteostatData, StationReadings
from src.database import create_postgres, create_postgres_session

def convert_to_local_time(time_utc):
    local_time = timezone('America/Asuncion')
    utc_minus_0 = timezone('UTC')
    time_utc = utc_minus_0.localize(time_utc)
    time_utc_local = time_utc.astimezone(local_time)
    return time_utc_local.replace(tzinfo=None) #timezone naive

def convert_to_utc(time_local):
    local_time = timezone('America/Asuncion') 
    time_localized = local_time.localize(time_local)
    utc_time = timezone('UTC')
    time_local_utc = time_localized.astimezone(utc_time)
    return time_local_utc.replace(tzinfo=None)


def fill_missing_dates(df, frequency = 'h'):
    complete_date_range = pd.date_range(start = df.index.min(), end = df.index.max(), freq=frequency)
    df = df.reindex(complete_date_range).mask(df == '')

    return df

def interpolate_data(df, limit = 24):
    numeric_columns = [col for col in df.select_dtypes(include=['float64', 'int64']).columns if col != 'ID']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors = 'coerce')

    df[numeric_columns] = df[numeric_columns].interpolate(method = 'linear', axis = 0, limit = limit).round(2)

    return df

def get_meteostat_data(start, end):
    print('retrieving meteorological data from meteostat...')
    asuncion = Point(-25.2667, -57.6333, 101) # long, lat, altitude

    data = Hourly(asuncion, start, end)
    data = data.fetch()
    print(data.info())

    # Convert time to UTC-3
    print('converting to local timezone...')
    data.index = data.index.map(convert_to_local_time)
    print('converted!')
    meteo_features = ['temp', 'rhum', 'pres', 'wspd', 'wdir']
    
    data = data[meteo_features]
    data.rename(columns={'temp': 'temperature', 
                         'rhum': 'humidity', 
                         'pres': 'pressure', 
                         'wspd': 'wind_speed', 
                         'wdir': 'wind_dir'}, inplace=True)
    
    #data = fill_missing_dates(data)
    #data = interpolate_data(data)
    data['wind_dir_cos'] = np.cos(2 * np.pi * data.wind_dir / 360)
    data['wind_dir_sin'] = np.sin(2 * np.pi * data.wind_dir / 360)
    data.drop('wind_dir', axis=1, inplace=True)
    data['date'] = data.index


    data = data.round(2)
    print(data.info())

    return data

def fill_meteostat_data():

    try:
        postgres_engine = create_postgres()
        with create_postgres_session(postgres_engine) as session:

            if session.query(MeteostatData).count() == 0:
                print('No previous meteostat data')
                #first_stationreadings_timestamp = session.query(func.min(StationReadings.date)).scalar()
                last_stationreadings_timestamp = session.query(func.max(StationReadings.date)).scalar()

                #start_utc = convert_to_utc(first_stationreadings_timestamp)
                start_utc = datetime(2019,1,1,0,0,0,0)
                end_utc = convert_to_utc(last_stationreadings_timestamp)
            
            else:
                last_meteostat_timestamp = session.query(func.max(MeteostatData.date)).scalar()
                start_utc = convert_to_utc(last_meteostat_timestamp + timedelta(hours=1))
                end_utc = datetime.now(timezone('UTC')).replace(tzinfo = None, minute = 0, second = 0, microsecond = 0)
                print(f'Getting meteostat data from {start_utc} to {end_utc} (UTC)')
                
            if start_utc < end_utc:
                print(start_utc)
                print(end_utc)
                meteostat_df = get_meteostat_data(start=start_utc, end = end_utc)

                rows_before = session.query(MeteostatData).count()
                
                meteostat_df.to_sql('meteostat_data', session.connection(), if_exists='append', index = False )

                rows_after = session.query(MeteostatData).count()

                print(f'Rows before insertion {rows_before}')
                print(f'Rows after insertion: {rows_after}')
                session.commit()
            else:
                print('no new meteostat data')
    except Exception as e:
        print('An error occurred: ', e)
    finally:
        session.close()
