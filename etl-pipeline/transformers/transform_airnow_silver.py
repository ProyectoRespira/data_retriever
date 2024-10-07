import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def combine_existing_and_new_readings(df1, df2, klogger=None):
    try:
        df_combined = pd.concat([df1, df2], ignore_index=True)
        df_combined['date_utc'] = pd.to_datetime(df_combined['date_utc'], errors='coerce')
        return df_combined
    except Exception as e:
        klogger.error(f"Error combining readings: {e}")

def drop_bad_readings(df, klogger=None):
    try:
        df['pm2_5'] = df['pm2_5'].replace(-999, np.nan)
        return df
    except Exception as e:
        klogger.error(f"Error dropping bad readings: {e}")

def interpolate_missing_data(df, klogger=None):
    try:
        # interpolation for numerical columns
        df_interpolated = df
        df_interpolated['pm2_5'] = df_interpolated['pm2_5'].interpolate(method='linear', limit_direction='both')

        df_interpolated['station_id'] = df_interpolated['station_id'].bfill().ffill()
        
        # Fill NaN values in data_source with 'interpolated'
        df_interpolated['data_source'].fillna('interpolated', inplace=True)
        
        # Reset index to ensure date_utc is a column
        df_interpolated = df_interpolated.reset_index()
        
        return df_interpolated
    except Exception as e:
        klogger.error(f"Error interpolating missing data: {e}")

def set_variable_dtypes(df, klogger=None):
    try:
        df = df.astype({
            'measurement_id': 'Int64',  # Use 'Int64' for nullable integers
            'pm2_5': 'float',
            'station_id': 'int'
        })
        df['date_utc'] = pd.to_datetime(df['date_utc'])
        return df
    except Exception as e:
        klogger.error(f"Error in setting variable types: {e}")

def process_weather_silver(group, klogger=None):
    try:
        group.drop_duplicates(subset=['date_utc'], keep='first', inplace=True)
        group = drop_bad_readings(group, klogger=klogger)
        group.set_index('date_utc', inplace=True)
        group['data_source'] = 'raw' 

        group = group.resample('h').asfreq()

        group = interpolate_missing_data(group, klogger=klogger)

        group = set_variable_dtypes(group, klogger=klogger)
        group.sort_values(by=['date_utc'], ascending=True, inplace=True)

        return group
    except Exception as e:
        klogger.error(f"Error processing weather silver group: {e}")

@transformer
def transform(data, data_2, *args, **kwargs):
    klogger = kwargs.get('logger')

    try:
        if not data_2.empty:
            group = combine_existing_and_new_readings(data, data_2, klogger=klogger)
        else:
            group = data

        processed_data = group.groupby('station_id').apply(lambda x: process_weather_silver(x, klogger=klogger)).reset_index(drop=True)

        if not data_2.empty:
            data_2_filtered = data_2[['station_id', 'date_utc']]
            processed_data = processed_data.merge(data_2_filtered, on=['station_id', 'date_utc'], how='left', indicator=True)
            processed_data = processed_data[processed_data['_merge'] == 'left_only'].drop(columns=['_merge'])

        return processed_data
    except Exception as e:
        if klogger:
            klogger.error(f"Error in transform: {e}")
        raise

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'