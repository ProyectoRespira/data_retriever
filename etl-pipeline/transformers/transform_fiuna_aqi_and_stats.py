if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

def calculate_aqi_2_5_and_level(x):
    """
    Calculate AQI and level for PM2.5 based on the provided value.
    """
    if x <= 12:
        return round(x * 50 / 12, 0), 1  # Good
    elif x <= 35.4:
        return round(51 + (x - 12.1) * 49 / 23.3, 0), 2  # Moderate
    elif x <= 55.4:
        return round(101 + (x - 35.5) * 49 / 19.9, 0), 3  # Unhealthy for sensitive groups
    elif x <= 150.4:
        return round(151 + (x - 55.5) * 49 / 94.4, 0), 4  # Unhealthy
    elif x <= 250.4:
        return round(201 + (x - 150.5) * 99 / 99.9, 0), 5  # Very unhealthy
    elif x <= 350.4:
        return round(301 + (x - 250.5) * 99 / 99.9, 0), 6  # Hazardous
    else:
        return round(401 + (x - 350.5) * 99 / 149.9, 0), 7  # Beyond AQI

def calculate_aqi_10(x):
    """
    Calculate AQI for PM10 based on the provided value.
    """
    if x <= 54:
        return round(x * 50 / 54, 0)
    elif x <= 154:
        return round(51 + (x - 55) * 49 / 99, 0)
    elif x <= 254:
        return round(101 + (x - 155) * 49 / 99, 0)
    elif x <= 354:
        return round(151 + (x - 255) * 49 / 99, 0)
    elif x <= 424:
        return round(201 + (x - 355) * 99 / 69, 0)
    elif x <= 504:
        return round(301 + (x - 425) * 99 / 79, 0)
    else:
        return round(401 + (x - 504) * 99 / 100, 0)

def calculate_aqi_pm2_5(row):
    """
    Apply AQI calculation for PM2.5 if AQI value is NaN.
    """
    if pd.isna(row['aqi_pm2_5']) and pd.notna(row['pm2_5_24h_mean']):
        aqi, level = calculate_aqi_2_5_and_level(row['pm2_5_24h_mean'])
        return pd.Series([aqi, level], index=['aqi_pm2_5', 'aqi_level'])
    else:
        return pd.Series([row['aqi_pm2_5'], row['aqi_level']], index=['aqi_pm2_5', 'aqi_level'])

def calculate_aqi_pm10(row):
    """
    Apply AQI calculation for PM10 if AQI value is NaN.
    """
    if pd.isna(row['aqi_pm10']) and pd.notna(row['pm10_24h_mean']):
        return calculate_aqi_10(row['pm10_24h_mean'])
    else:
        return row['aqi_pm10']

def calculate_statistics(df):
    """
    Calculate additional statistics for the PM2.5 and AQI columns.
    """
    df['pm2_5_avg_6h'] = df['pm2_5'].rolling(window=6, min_periods=1).mean()
    df['pm2_5_max_6h'] = df['pm2_5'].rolling(window=6, min_periods=1).max()
    df['pm2_5_skew_6h'] = df['pm2_5'].rolling(window=6, min_periods=1).skew()
    df['pm2_5_std_6h'] = df['pm2_5'].rolling(window=6, min_periods=1).std()
    
    df['aqi_pm2_5_max_24h'] = df['aqi_pm2_5'].rolling(window=24, min_periods=1).max()
    df['aqi_pm2_5_skew_24h'] = df['aqi_pm2_5'].rolling(window=24, min_periods=1).skew()
    df['aqi_pm2_5_std_24h'] = df['aqi_pm2_5'].rolling(window=24, min_periods=1).std()
    
    columns_to_backfill = [
        'pm2_5_avg_6h', 'pm2_5_max_6h', 'pm2_5_skew_6h', 'pm2_5_std_6h', 
        'aqi_pm2_5_max_24h', 'aqi_pm2_5_skew_24h', 'aqi_pm2_5_std_24h', 'aqi_pm2_5'
    ]
    
    df[columns_to_backfill] = df[columns_to_backfill].bfill().ffill()
    return df

def process_data(data):
    # averages for AQI
    data['pm2_5_24h_mean'] = data['pm2_5'].rolling(window=24, min_periods=1).mean()
    data['pm10_24h_mean'] = data['pm10'].rolling(window=24, min_periods=1).mean()
    
    # aqi pm2.5
    data[['aqi_pm2_5', 'aqi_level']] = data.apply(calculate_aqi_pm2_5, axis=1)

    # aqi pm10
    data['aqi_pm10'] = data.apply(calculate_aqi_pm10, axis=1)

    # other stats
    data = calculate_statistics(data)

    data = data.drop(columns=['pm2_5_24h_mean', 'pm10_24h_mean'])
    # sort data
    data.sort_values(by=['date_utc'], ascending=False, inplace=True)
    data = data[data['in_24h_interval'] != 1]
    data = data.drop('in_24h_interval', axis=1)
    data.dropna(inplace = True)
    return data

@transformer
def transform(data, *args, **kwargs):
    
    klogger = kwargs.get('logger')
    try:
        if data.empty:
            klogger.exception('Dataframe is empty')
            return data
        data =  process_data(data)
    except Exception as e:
        klogger.exception(e)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'