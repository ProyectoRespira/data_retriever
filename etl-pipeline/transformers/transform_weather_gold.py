from pytz import timezone
import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def convert_to_local_time(time_utc):
    local_time = timezone('America/Asuncion')
    time_utc_local = time_utc.astimezone(local_time)
    return time_utc_local

def convert_angles_to_cos_sin(df):
    df['wind_dir_cos'] = np.cos(2 * np.pi * df.wind_dir / 360)
    df['wind_dir_sin'] = np.sin(2 * np.pi * df.wind_dir / 360)
    df.drop('wind_dir', axis=1, inplace=True)

    return df

def process_data(df):
    df['date_localtime'] = df['date_utc'].apply(convert_to_local_time)
    df.drop(columns = ['date_utc'], inplace = True)
    df = convert_angles_to_cos_sin(df)
    df = df.drop_duplicates(subset='date_localtime', keep='first')
    df.sort_values(by=['date_localtime'], ascending = False, inplace=True)
    df.rename(columns={'id':'measurement_id'}, inplace = True)
    
    return df

@transformer
def transform(data, *args, **kwargs):
    # Specify your transformation logic here
    processed_data = data.groupby('weather_station').apply(process_data).reset_index(drop=True)

    return processed_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'