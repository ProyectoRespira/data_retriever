from sqlalchemy.orm import Session
from datetime import datetime
import pandas as pd

def get_timerange_with_missing_stats(session: Session, station_id: int) -> tuple:
    '''
    Get the maximum and minimum dates for a specific station where stats values
    need to be calculated
    '''
    pass

def get_data_for_calculating_stats(session: Session, station_id: int, start: datetime, end: datetime) -> pd.DataFrame:
    '''
    Fetch readings for a specific station where stats values need to be updated.
    '''
    pass

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

def update_station_readings_stats(session, df) -> bool:
    '''
    update stats in station_readings table
    '''
    pass