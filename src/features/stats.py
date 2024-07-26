from sqlalchemy.orm import Session
from sqlalchemy import update, func, or_
from src.models import StationReadings
from datetime import datetime, timedelta
import pandas as pd

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_timerange_with_missing_stats(session: Session, station_id: int, last_transformation_timestamp) -> tuple:
    '''
    Get the maximum and minimum dates for a specific station where stats values
    need to be calculated
    '''
    result = session.query(
        func.min(StationReadings.date).label('min_date'),
        func.max(StationReadings.date).label('max_date')
    ).filter(
        StationReadings.station == station_id,
        StationReadings.date >= last_transformation_timestamp,
        or_(StationReadings.aqi_pm2_5_max_24h == None, 
            StationReadings.aqi_pm2_5_skew_24h == None,
            StationReadings.aqi_pm2_5_std_24h == None,

            StationReadings.pm2_5_avg_6h == None,
            StationReadings.pm2_5_max_6h == None,
            StationReadings.pm2_5_skew_6h == None,
            StationReadings.pm2_5_std_6h == None)
    ).one()

    return result.min_date, result.max_date

def get_data_for_calculating_stats(session: Session, station_id: int, start: datetime, end: datetime) -> pd.DataFrame:
    '''
    Fetch readings for a specific station where stats values need to be updated.
    '''
    readings = session.query(StationReadings.date,
                            StationReadings.id,
                            StationReadings.aqi_pm2_5,
                            StationReadings.pm2_5,
                            StationReadings.station
                            ).filter(
                                    StationReadings.station == station_id,
                                    StationReadings.date >= start - timedelta(hours=24),
                                    StationReadings.date <= end
                                    ).all()
    
    df = pd.DataFrame([{'date': reading.date, 
                        'id': reading.id,
                        'aqi_pm2_5' : reading.aqi_pm2_5, 
                        'pm2_5': reading.pm2_5,
                        'station': reading.station} for reading in readings])
    df.set_index('date', inplace=True)
    df.sort_index(ascending=True, inplace=True)
    df['pm2_5'] = df['pm2_5'].ffill()
    df['aqi_pm2_5'] = df['aqi_pm2_5'].ffill()

    df.reset_index(inplace=True)
    df.rename(columns={'index': 'date'}, inplace=True)

    return df

def calculate_station_readings_stats(session: Session, station_id: int, last_transformation_timestamp: datetime) -> pd.DataFrame:
    '''
    update station_readings with missing stats.
    '''
    # get data for performing calculations
    start, end = get_timerange_with_missing_stats(session, station_id, last_transformation_timestamp)
    df = get_data_for_calculating_stats(session, station_id, start, end)

    # calculate necessary stats using pandas:
    
    # pm2_5_avg_6h = Column(Float)
    df['pm2_5_avg_6h'] = df['pm2_5'].rolling(window = 6, min_periods=6).mean()
    df['pm2_5_max_6h'] = df['pm2_5'].rolling(window = 6, min_periods=6).max()
    df['pm2_5_skew_6h'] = df['pm2_5'].rolling(window = 6, min_periods=6).skew()
    df['pm2_5_std_6h'] = df['pm2_5'].rolling(window = 6, min_periods=6).std()
    
    # aqi_pm2_5_max_24h = Column(Float)
    df['aqi_pm2_5_max_24h'] = df['aqi_pm2_5'].rolling(window = 24, min_periods=24).max()
    df['aqi_pm2_5_skew_24h'] = df['aqi_pm2_5'].rolling(window = 24, min_periods=24).skew()
    df['aqi_pm2_5_std_24h'] = df['aqi_pm2_5'].rolling(window = 24, min_periods=24).std()

    df = df.round(2)
    df.drop(columns=['pm2_5', 'aqi_pm2_5'], axis=1, inplace=True)
    df.dropna(axis=0, how='any', inplace=True)
    
    return df

def update_station_readings_stats(session, station_id, last_transformation_timestamp) -> bool:
    '''
    update stats in station_readings table
    '''
    try:
        logging.info(f'Starting stats calculation for station {station_id}')
        df = calculate_station_readings_stats(session, station_id, last_transformation_timestamp)

        update_dict = df.to_dict(orient='records')
        session.execute(
            update(StationReadings), update_dict
        )

        logging.info('Stats update complete')
        return True
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        return False