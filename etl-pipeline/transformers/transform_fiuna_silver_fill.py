import pandas as pd
import numpy as np
from pytz import timezone

def add_date_column_as_index(df):
    '''
    Converts 'fecha' and 'hora' (VARCHAR) columns in dataframe to
    'date' datetime column.
    '''
    df['date_localtime'] = df['date'] + ' ' + df['hour']
    df['date_localtime'] = pd.to_datetime(df['date_localtime'], format='%d-%m-%Y %H:%M', errors='coerce')
    
    local_time_zone = timezone('America/Asuncion')
    df['date_localtime'] = df['date_localtime'].apply(
        lambda x: x.tz_localize(local_time_zone, 
                                ambiguous='NaT', 
                                nonexistent='shift_forward') if pd.notnull(x) else x
        )
    df.set_index('date_localtime', inplace=True)
    
    df.sort_index(inplace=True)
    return df.drop(columns=['date', 'hour'], axis=1)

def resample_to_5min(df):
    df = df[~df.index.duplicated(keep='first')]
    df_resampled = df.resample('5min').asfreq()
    return df_resampled

def fill_missing_values(df, columns):
    df_interpolated = df.copy()
    df_interpolated[columns] = df[columns].interpolate(method='linear', limit=12)
    df_interpolated['data_source'] = df_interpolated['data_source'].fillna('interpolated')
    df_interpolated['station_id'] = df_interpolated['station_id'].bfill().ffill()
    df_interpolated.dropna(how='any', subset=['pm2_5', 'pm10', 'pm1'], inplace = True)
    return df_interpolated

def process_data(df):
    # add date and hour columns as index
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
    
    # reset index and sort by 'date_localtime'
    df.reset_index(inplace=True)
    df.sort_values(by=['date_localtime'], inplace=True)
    
    # drop duplicates based on 'date_localtime'
    df.drop_duplicates(subset=['date_localtime'], keep='first', inplace=True)

    # drop rows where 'date_localtime' is NaT
    df = df[df['date_localtime'] != pd.NaT]
    df = df.sort_values(by=['date_localtime'], ascending = True)
    
    return df

@transformer
def transform(data, *args, **kwargs):
    klogger = kwargs.get('logger')

    try:
        if data.empty:
            raise Exception('Dataframe is empty')
        data = process_data(data)
    except Exception as e:
        if klogger:
            klogger.exception(e)
        return None

    return data



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'