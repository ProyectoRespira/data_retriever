import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def combine_existing_and_new_readings(df1, df2, logger):
    try:
        logger.info('Combining existing and new readings.')
        df_combined = pd.concat([df1, df2], ignore_index=True)
        df_combined['date_utc'] = pd.to_datetime(df_combined['date_utc'], errors='coerce')
        logger.info(f"Combined DataFrame has {len(df_combined)} rows.")
        return df_combined
    except Exception as e:
        logger.error(f"Error in combining existing and new readings: {e}")

def drop_bad_readings(df, logger):
    try:
        logger.info('Dropping bad readings based on predefined thresholds.')
        df['temperature'] = df['temperature'].where(df['temperature'].between(-5, 50), np.nan)
        df['humidity'] = df['humidity'].where(df['humidity'].between(0, 100), np.nan)
        df['pressure'] = df['pressure'].where(df['pressure'].between(900, 1200), np.nan)
        df['wind_speed'] = df['wind_speed'].where(df['wind_speed'].between(0, 200), np.nan)
        df['wind_dir'] = df['wind_dir'].where(df['wind_dir'].between(0, 360), np.nan)
        logger.info('Completed dropping bad readings.')
        return df
    except Exception as e:
        logger.error(f"Error in dropping bad readings: {e}")

def interpolate_missing_data(df, logger):
    try:
        logger.info('Interpolating missing data.')
        df_interpolated = df
        df_interpolated[['temperature', 'pressure', 'humidity', 'wind_speed']] = df_interpolated[['temperature', 'pressure', 'humidity', 'wind_speed']].interpolate(method='linear', limit_direction='both')
        df_interpolated['wind_dir'] = df['wind_dir'].bfill().ffill()
        df_interpolated['weather_station'] = df['weather_station'].bfill().ffill()
        df_interpolated['data_source'].fillna('interpolated', inplace=True)
        df_interpolated = df_interpolated.reset_index()
        logger.info('Interpolation completed.')
        return df_interpolated
    except Exception as e:
        logger.error(f"Error in interpolating missing data: {e}")

def set_variable_dtypes(df, logger):
    try:
        logger.info('Setting variable data types.')
        df = df.astype({
            'measurement_id': 'Int64',
            'temperature': 'float',
            'pressure': 'float',
            'humidity': 'float',
            'wind_speed': 'float',
            'wind_dir': 'float',
            'weather_station': 'int'
        })
        df['date_utc'] = pd.to_datetime(df['date_utc'])
        return df
    except Exception as e:
        logger.error(f"Error in setting variable types: {e}")
    

def process_weather_silver(group, logger):
    try:
        logger.info(f"Processing group for weather station: {group['weather_station'].iloc[0]}")
        
        group.drop_duplicates(subset=['date_utc'], keep='first', inplace=True)
        group = drop_bad_readings(group, logger=logger)
        group.set_index('date_utc', inplace=True)
        group['data_source'] = 'raw' 

        group = group.resample('h').asfreq()
        group = interpolate_missing_data(group, logger=logger)
        group = set_variable_dtypes(group, logger=logger)
        group.sort_values(by=['date_utc'], ascending=True, inplace=True)

        logger.info(f"Completed processing for weather station: {group['weather_station'].iloc[0]}")
        
        return group
    except Exception as e:
        logger.error(f"Error in processing weather silver: {e}")

@transformer
def transform(data, data_2, *args, **kwargs):
    
    klogger = kwargs.get('logger')
    try:

        klogger.info('Starting transformation process.')
        
        if not data_2.empty:
            klogger.info('Combining new and existing data.')
            group = combine_existing_and_new_readings(data, data_2, logger=klogger)
        else:
            klogger.info('No existing data for interpolation. Using new data only.')
            group = data

        processed_data = group.groupby('weather_station').apply(process_weather_silver, logger=klogger).reset_index(drop=True)

        if not data_2.empty:
            klogger.info('Filtering out existing data to avoid duplication.')
            data_2_filtered = data_2[['weather_station', 'date_utc']]
            processed_data = processed_data.merge(data_2_filtered, on=['weather_station', 'date_utc'], how='left', indicator=True)
            processed_data = processed_data[processed_data['_merge'] == 'left_only'].drop(columns=['_merge'])
            klogger.info('Transformation process completed.')
        
        return processed_data
    except Exception as e:
        klogger.error(f"Error in transformation process: {e}")
        raise

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'