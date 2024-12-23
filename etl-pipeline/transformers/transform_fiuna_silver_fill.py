import pandas as pd
import numpy as np
from pytz import timezone

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
    
def add_date_column_as_index(df):
    '''
    Converts 'fecha' and 'hora' (VARCHAR) columns in dataframe to
    'date' datetime column.
    '''
    df['date_utc'] = df['date'] + ' ' + df['hour']
    df['date_utc'] = pd.to_datetime(df['date_utc'], format='%d-%m-%Y %H:%M', errors='coerce')
    
    local_time_zone = timezone('America/Asuncion')
    df['date_utc'] = df['date_utc'].apply(
        lambda x: x.tz_localize(local_time_zone, 
                                ambiguous='NaT', 
                                nonexistent='shift_forward') if pd.notnull(x) else x
        )
    df.set_index('date_utc', inplace=True)
    
    df.sort_index(inplace=True)
    return df.drop(columns=['date', 'hour'], axis=1)

def resample_to_5min(df):
    df = df[~df.index.duplicated(keep='first')]
    df_resampled = df.resample('5min').asfreq()
    return df_resampled

def fill_missing_values(df, columns):
    df_interpolated = df
    df_interpolated[columns] = df_interpolated[columns].interpolate(method='linear', limit=12)
    df_interpolated['data_source'] = df_interpolated['data_source'].fillna('interpolated')
    df_interpolated['station_id'] = df_interpolated['station_id'].bfill().ffill()
    df_interpolated.dropna(how='any', subset=['pm2_5', 'pm10', 'pm1'], inplace = True)
    return df_interpolated

def process_data(df):
    # add date and hour columns as index
    df.drop(columns=['processed_to_silver'], axis = 1, inplace = True)
    df['data_source'] = 'raw'
    df = add_date_column_as_index(df)
    
    # resample data to 5-minute intervals
    df = resample_to_5min(df)
    
    # fill missing values
    columns = ['pm1', 'pm2_5', 'pm10', 'temperature', 'humidity', 'pressure']
    df = fill_missing_values(df, columns)
    
    # process measurement_id and station_id
    df['measurement_id'] = df['measurement_id'].astype('Int64')
    df['station_id'] = df['station_id'].astype(int)
    
    # reset index and sort by 'date_utc'
    df.reset_index(inplace=True)
    df.sort_values(by=['date_utc'], inplace=True)
    
    # drop duplicates based on 'date_utc'
    df.drop_duplicates(subset=['date_utc'], keep='first', inplace=True)

    # drop rows where 'date_utc' is NaT
    df = df[df['date_utc'] != pd.NaT]
    df = df.dropna(subset=['date_utc'])
    df = df.sort_values(by=['date_utc'], ascending = True) 
    
    return df

@transformer
def transform(data, *args, **kwargs):
    klogger = kwargs.get('logger')
    try:
        if data.empty:
            klogger.exception('Dataframe is empty')
            return data
        data = process_data(data)
        return data
    except Exception as e:
        klogger.exception(e)