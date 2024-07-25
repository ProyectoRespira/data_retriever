from src.models import StationReadings
from datetime import timedelta
from sqlalchemy import update, func, or_
import pandas as pd

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# AQI functions

def calculate_aqi_2_5_and_level(x):
    if x <= 12:
        return round(x * 50 / 12, 0), 1 # good
    elif x <= 35.4:
        return round(51 + (x - 12.1) * 49 / 23.3, 0), 2 # moderate
    elif x <= 55.4:
        return round(101 + (x - 35.5) * 49 / 19.9, 0), 3 # unhealthy for sensitive groups
    elif x <= 150.4:
        return round(151 + (x - 55.5) * 49 / 94.4, 0), 4 # unhealthy
    elif x <= 250.4:
        return round(201 + (x - 150.5) * 99 / 99.9, 0), 5 # very unhealthy
    elif x <= 350.4:
        return round(301 + (x - 250.5) * 99 / 99.9, 0), 6 # hazardous
    else:
        return round(401 + (x - 350.5) * 99 / 149.9, 0), 7 # beyond AQI

def calculate_aqi_10(x):
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
    
def get_timerange_with_missing_aqi(session, station_id, last_transformation_timestamp): 
    '''
    Get the maximum and minimum dates for a specific station where AQI values
    need to be calculated
    '''
    result = session.query(
        func.min(StationReadings.date).label('min_date'),
        func.max(StationReadings.date).label('max_date')
    ).filter(
        StationReadings.station == station_id,
        StationReadings.date >= last_transformation_timestamp,
        or_(StationReadings.aqi_pm2_5 == None, StationReadings.aqi_pm10 == None)
    ).one()

    return result.min_date, result.max_date

def get_station_readings_for_aqi_calculation(session, station_id, start, end):
    '''
    Fetch readings for a specific station where AQI values need to be updated.
    '''
    readings = session.query(StationReadings.date,
                             StationReadings.id,
                        StationReadings.pm10,
                         StationReadings.pm2_5,
                         StationReadings.station
                         ).filter(
                                    StationReadings.station == station_id,
                                    StationReadings.date >= start - timedelta(hours=24),
                                    StationReadings.date <= end
                                ).all()
    
    df = pd.DataFrame([{'date': reading.date, 'id': reading.id ,'pm10' : reading.pm10, 'pm2_5': reading.pm2_5, 'station': reading.station} for reading in readings])

    df.set_index('date', inplace=True)
    df.sort_index(ascending=True, inplace=True)
    df['pm2_5'] = df['pm2_5'].ffill()
    df['pm10'] = df['pm10'].ffill()

    df.reset_index(inplace=True)
    df.rename(columns={'index': 'date'}, inplace=True)
    logging.info('Data for calculating AQI')
    print(df.info())

    return df


def compute_and_update_aqi_for_station_readings(session, station_id, last_transformation_timestamp):
    '''
    Calculate AQI 2.5 and AQI 10 for existing pm readings in StationReadings
    and update table with AQI values.
    '''
    try: 
        logging.info(f'Starting AQI calculation for station {station_id}')
        start, end = get_timerange_with_missing_aqi(session, station_id, last_transformation_timestamp)

        df = get_station_readings_for_aqi_calculation(session, station_id, start, end)

        logging.info(f'Calculating... ')

        df['pm2_5_24h_mean'] = df['pm2_5'].rolling(window=24, min_periods=24).mean()
        df['pm10_24h_mean'] = df['pm10'].rolling(window=24, min_periods=24).mean()
        
        df[['aqi_pm2_5', 'level']] = df['pm2_5_24h_mean'].apply(lambda x: pd.Series(calculate_aqi_2_5_and_level(x)))
        df['aqi_pm10'] = df['pm10_24h_mean'].apply(calculate_aqi_10)

        df.drop(columns=['pm2_5_24h_mean', 'pm10_24h_mean', 'pm2_5', 'pm10'],
                axis=1,
                inplace=True)
        
        df.dropna(axis=0, how='any', inplace=True)
        print(df.head())

        logging.info(f'Inserting...')
        
        update_dict = df.to_dict(orient='records')
        session.execute(
            update(StationReadings), update_dict
        )

        #session.commit()
        logging.info('AQI update completed.')
        return True
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        return False