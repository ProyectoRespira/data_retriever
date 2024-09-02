import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def add_date_column_as_index(df):
    '''
    Converts 'fecha' and 'hora' (VARCHAR) columns in dataframe to
    'date' datetime column.
    '''
    df['date_localtime'] = df['date'] + ' ' + df['hour']
    df['date_localtime'] = pd.to_datetime(df['date_localtime'], format = '%d-%m-%Y %H:%M', errors = 'coerce')
    df.set_index('date_localtime', inplace = True)
    return df.drop(columns = ['date', 'hour'], axis = 1)

def resample_to_5min(df):
    df = df[~df.index.duplicated(keep='first')]
    df_resampled = df.resample('5min').asfreq()
    return df_resampled

def fill_missing_values(df, columns):
    df_interpolated = df.copy()
    df_interpolated[columns] = df[columns].interpolate(method = 'linear')
    df_interpolated['station_id'] = df_interpolated['station_id'].bfill().ffill()

    return df_interpolated

def process_data(df):
    df = add_date_column_as_index(df)
    print(df.head())
    df = resample_to_5min(df)

    columns = ['pm1', 'pm2_5', 'pm10', 'temperature', 'humidity', 'pressure']
    df = fill_missing_values(df, columns)
    df['measurement_id'] = df['measurement_id'].fillna(0).astype(int)
    df['station_id'] = df['station_id'].astype(int)
    df.reset_index(inplace = True)
    
    return df



@transformer
def transform(data, *args, **kwargs):

    # Specify your transformation logic here
    klogger = kwargs.get('logger')

    try:
        if data.empty:
            raise Exception('Dataframe is empty')
        data = process_data(data)
    except Exception as e:
        klogger.exception(e)
    
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'