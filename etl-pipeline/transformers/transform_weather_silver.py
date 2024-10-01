import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def combine_existing_and_new_readings(df1, df2):
    df_combined = pd.concat([df1, df2], ignore_index=True)
    df_combined['date_utc'] = pd.to_datetime(df_combined['date_utc'], errors='coerce')
    return df_combined

def drop_bad_readings(df):
    df['temperature'] = df['temperature'].where(df['temperature'].between(-5, 50), np.nan)
    df['humidity'] = df['humidity'].where(df['humidity'].between(0, 100), np.nan)
    df['pressure'] = df['pressure'].where(df['pressure'].between(900, 1200), np.nan)
    df['wind_speed'] = df['wind_speed'].where(df['wind_speed'].between(0, 200), np.nan)
    df['wind_dir'] = df['wind_dir'].where(df['wind_dir'].between(0, 360), np.nan)
    return df

def interpolate_missing_data(df):
    
    # Perform interpolation for numerical columns
    df_interpolated = df.copy()
    df_interpolated[['temperature', 'pressure', 'humidity', 'wind_speed']] = df_interpolated[['temperature', 'pressure', 'humidity', 'wind_speed']].interpolate(method='linear', limit_direction='both')
    # Forward and backward fill for wind_dir
    df_interpolated['wind_dir'] = df['wind_dir'].bfill().ffill()
    df_interpolated['weather_station'] = df['weather_station'].bfill().ffill()
    
    # Fill NaN values in data_source with 'interpolated'
    df_interpolated['data_source'].fillna('interpolated', inplace=True)
    
    # Reset index to ensure date_utc is a column
    df_interpolated = df_interpolated.reset_index()
    
    return df_interpolated

def set_variable_dtypes(df):
    try:
        df = df.astype({
            'measurement_id': 'Int64',  # Use 'Int64' for nullable integers
            'temperature': 'float',
            'pressure': 'float',
            'humidity': 'float',
            'wind_speed': 'float',
            'wind_dir': 'float',
            'weather_station': 'int'
        })
        df['date_utc'] = pd.to_datetime(df['date_utc'])
    except Exception as e:
        print(f"Error in setting variable types: {e}")
    return df

def process_weather_silver(group):
    group.drop_duplicates(subset=['date_utc'], keep='first', inplace=True)
    group = drop_bad_readings(group)
    group.set_index('date_utc', inplace=True)
    group['data_source'] = 'raw' 
    
    group = group.resample('h').asfreq()
    
    group = interpolate_missing_data(group)
    
    group = set_variable_dtypes(group)
    group.sort_values(by=['date_utc'], ascending=True, inplace=True)

    return group

@transformer
def transform(data, data_2, *args, **kwargs):
    if not data_2.empty:
        group = combine_existing_and_new_readings(data, data_2)
    else:
        group = data.copy()

    processed_data = group.groupby('weather_station').apply(process_weather_silver).reset_index(drop=True)

    if not data_2.empty:
        data_2_filtered = data_2[['weather_station', 'date_utc']]
        processed_data = processed_data.merge(data_2_filtered, on=['weather_station', 'date_utc'], how='left', indicator=True)
        processed_data = processed_data[processed_data['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    return processed_data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'