from sqlalchemy.orm import Session
from sqlalchemy import update, func, or_
from src.models import StationReadings
from datetime import datetime, timedelta
import pandas as pd

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_timerange_with_missing_stats(session: Session, station_id: int) -> tuple:
    '''
    Get the maximum and minimum dates for a specific station where stats values
    need to be calculated
    '''
    result = session.query(
        func.min(StationReadings.date).label('min_date'),
        func.max(StationReadings.date).label('max_date')
    ).filter(
        StationReadings.station == station_id,
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
                            StationReadings.pm2_5
                            ).filter(
                                    StationReadings.station == station_id,
                                    StationReadings.date >= start - timedelta(hours=24),
                                    StationReadings.date <= end
                                    ).all()
    
    df = pd.DataFrame([{'date': reading.date, 
                        'id': reading.id,
                        'aqi_pm2_5' : reading.aqi_pm2_5, 
                        'pm2_5': reading.pm2_5} for reading in readings])

    return df

def calculate_station_readings_stats(session: Session, station_id: int) -> pd.DataFrame:
    '''
    update station_readings with missing stats.
    '''
    # get data for performing calculations
    start, end = get_timerange_with_missing_stats(session, station_id)
    df = get_data_for_calculating_stats(session, station_id, start, end)

    # calculate necessary stats using pandas:
    
    # pm2_5_avg_6h = Column(Float)
    df['pm2_5_avg_6h'] = df['pm2_5'].rolling(window = 6).mean()
    # pm2_5_max_6h = Column(Float)
    df['pm2_5_max_6h'] = df['pm2_5'].rolling(window = 6).max()
    # pm2_5_skew_6h = Column(Float)
    df['pm2_5_skew_6h'] = df['pm2_5'].rolling(window = 6).skew()
    # pm2_5_std_6h = Column(Float)
    df['pm2_5_std_6h'] = df['pm2_5'].rolling(window = 6).std()
    
    
    # aqi_pm2_5_max_24h = Column(Float)
    df['aqi_pm2_5_max_24h'] = df['aqi_pm2_5'].rolling(window = 24).max()
    # aqi_pm2_5_skew_24h = Column(Float)
    df['aqi_pm2_5_skew_24h'] = df['aqi_pm2_5'].rolling(window = 24).skew()
    # aqi_pm2_5_std_24h = Column(Float)
    df['aqi_pm2_5_std_24h'] = df['aqi_pm2_5'].rolling(window = 24).std()

    return df

def update_station_readings_stats(session, station_id) -> bool:
    '''
    update stats in station_readings table
    '''

    try:
        logging.info(f'Starting stats calculation for station {station_id}')
        df = calculate_station_readings_stats(session, station_id)

        update_dict = df.to_dict(orient='records')
        session.execute(
            update(StationReadings), update_dict
        )

        logging.info('Stats update complete')
        return True
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        return False