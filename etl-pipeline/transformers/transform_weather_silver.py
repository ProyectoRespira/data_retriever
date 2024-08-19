import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def select_relevant_features(df):
    meteo_features = ['date_utc', 'station_id', 'temperature', 'rhum', 'pres', 'wspd', 'wdir']
    return df[meteo_features]

def rename_meteostat_columns(df):
    return df.rename(columns={
            'rhum': 'humidity',
            'pres': 'pressure',
            'wspd': 'wind_speed',
            'wdir': 'wind_dir',
            'station_id': 'weather_station'
        })

def combine_existing_and_new_readings(df1, df2):
    df_combined = pd.concat([df1, df2])
    df_combined['date_utc'] = pd.to_datetime(df_combined['date_utc'])
    return df_combined

def preprocess_weather_bronze(df):
    df = select_relevant_features(df)
    df = rename_meteostat_columns(df)
    return df

def drop_bad_readings(df):
    df['temperature'] = df['temperature'].where(df['temperature'].between(-5, 50), np.nan)
    df['humidity'] = df['humidity'].where(df['humidity'].between(0, 100), np.nan)
    df['pressure'] = df['pressure'].where(df['pressure'].between(900, 1200), np.nan)
    df['wind_speed'] = df['wind_speed'].where(df['wind_speed'].between(0, 200), np.nan)
    df['wind_dir'] = df['wind_dir'].where(df['wind_dir'].between(0, 359), np.nan)
    return df

def interpolate_missing_data(df):
    df_interpolated = df.interpolate(method='linear')
    df_interpolated['wind_dir'] = df['wind_dir'].bfill().ffill()
    df_interpolated['weather_station'] = df['weather_station'].bfill().ffill()
    df_interpolated = df_interpolated.reset_index(names = 'date_utc')
    return df_interpolated

def set_variable_dtypes(df):
    df = df.astype({
    'date_utc': 'datetime64[ns]',  
    'weather_station': 'int',
    'temperature': 'float',
    'pressure': 'float',
    'humidity': 'float',
    'wind_speed': 'float',
    'wind_dir': 'float'            
    })
    return df

def process_weather_silver(group):
    group.drop_duplicates(subset = ['date_utc'], keep='first', inplace=True)
    group = drop_bad_readings(group)

    group.set_index('date_utc', inplace = True)
    group = group.resample('h').asfreq()
    group = interpolate_missing_data(group)
    
    group = set_variable_dtypes(group)
    group.sort_values(by=['date_utc'], ascending=True, inplace=True)

    return group
    

@transformer
def transform(data, data_2, *args, **kwargs):

    new_readings = preprocess_weather_bronze(data)

    if not data_2.empty:
        group = combine_existing_and_new_readings(new_readings, data_2)
    else:
        group = new_readings.copy()
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