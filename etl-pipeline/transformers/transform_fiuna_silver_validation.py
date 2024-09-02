import pandas as pd 
import numpy as np
from datetime import datetime
import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def validate_date_hour(date, hour):
    if date is None:
        return False
    if hour is None:
        return False
    
    date = date.strip()
    hour = hour.strip()

    date_pattern = re.compile(r'^\d{1,2}-\d{1,2}-\d{4}$')
    hour_pattern = re.compile(r'^\d{1,2}:\d{2}$')

    if not date_pattern.match(date):
        return False
    if not hour_pattern.match(hour):
        return False
    try:
        date_obj = datetime.strptime(date, '%d-%m-%Y')
        if date_obj.year < 2019:
            return False
    except ValueError:
        return False
    
    return True

def validate_pm_readings(pm):
    if pm >= 0:
        return pm
    else:
        return np.nan

def validate_pressure(pressure):
    if 800 < pressure < 1200:
        return pressure
    else:
        return np.nan

def validate_temperature(temperature):
    if -10 < temperature < 80:
        return temperature
    else:
        return np.nan

def validate_humidity(humidity):
    if 0 <= humidity <= 100:
        return humidity
    else:
        return np.nan

def rename_columns(df):
    new_names = {
        'fecha':'date',
        'hora':'hour',
        'mp1':'pm1',
        'mp2_5':'pm2_5',
        'mp10':'pm10',
        'temperatura':'temperature',
        'humedad':'humidity',
        'presion':'pressure'
    }
    df.rename(columns=new_names, inplace = True)
    return df

def convert_dtypes(df):
    # Convert 'date' and 'hour' to strings
    df['date'] = df['date'].astype(str)
    df['hour'] = df['hour'].astype(str)
    
    # Convert other columns to numeric, coercing errors to NaN
    df['pm1'] = pd.to_numeric(df['pm1'], errors='coerce')
    df['pm2_5'] = pd.to_numeric(df['pm2_5'], errors='coerce')
    df['pm10'] = pd.to_numeric(df['pm10'], errors='coerce')
    df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
    df['humidity'] = pd.to_numeric(df['humidity'], errors='coerce')
    df['pressure'] = pd.to_numeric(df['pressure'], errors='coerce')
    
    return df

def process_data(df):
    # rename columns
    df.dropna(inplace = True)
    df = rename_columns(df)
    df.drop(columns=['id','bateria'], axis = 1, inplace = True)
    # assert dtypes
    df = convert_dtypes(df)
    # validate readings 
    df = df[df.apply(lambda row: validate_date_hour(row['date'], row['hour']), axis = 1)]
    df['pressure'] = df['pressure'].apply(validate_pressure)
    df['temperature'] = df['temperature'].apply(validate_temperature)
    df['humidity'] = df['humidity'].apply(validate_humidity)
    df['pm1'] = df['pm1'].apply(validate_pm_readings)
    df['pm2_5'] = df['pm2_5'].apply(validate_pm_readings)
    df['pm10'] = df['pm10'].apply(validate_pm_readings)

    return df

@transformer
def transform(data, *args, **kwargs):
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