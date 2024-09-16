import pandas as pd
import numpy as np
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def change_frequency(df):
    df.index = df.index.floor('h')
    df = df.groupby([df.index, 'station']).agg({
        'pm1': 'mean',
        'pm2_5': 'mean',
        'pm10': 'mean',
        'weather_humidity': 'mean',
        'calibration_factor': 'mean'
    }).round(2)
    return df

def calibrate_pm(df):
    pm_columns = ['pm2_5', 'pm10', 'pm1']

    tolerance = 1e-9
    df['pm_calibrated'] = df['calibration_factor'].apply(
        lambda x: abs(x - 1) > tolerance
    )

    df['C_RH'] = df['weather_humidity'].apply(
        lambda x: 1 if (x < 65 or np.isnan(x)) else 
                (0.0121212 * x) + 1 if x < 85 else 
                (1 + ((0.2 / 1.65) / (-1 + 100 / min(x, 90))))
            )
    
    for pm in pm_columns:
        df[pm] = (df[pm] / df['C_RH']) * df['calibration_factor']
    
    df = df.round(2)
    df.drop(columns=['weather_humidity', 'C_RH', 'calibration_factor'], inplace = True)
    df.sort_index(ascending = True, inplace = True)
    df.reset_index(inplace=True)
    
    return df

def process_data(df):
    # add date column as index
    df.set_index('date_localtime', inplace = True)
    df.index = df.index.floor('h')
    df = change_frequency(df)
    print('change freq')
    df = calibrate_pm(df)
    print('calibrate')
    df = df.dropna()
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