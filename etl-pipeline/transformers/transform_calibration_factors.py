import pandas as pd 
import numpy as np 
from dateutil.relativedelta import relativedelta

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def check_data_coverage(df, date_column = 'date_utc', frequency = 'h', threshold=0.6):
    df[date_column] = pd.to_datetime(df[date_column])

    end_date = df[date_column].max()
    start_date = end_date - pd.DateOffset(months = 3)
    df_last_3_months = df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]
    
    expected_points = pd.date_range(start=start_date, end=end_date, freq=frequency).size
    actual_points = df_last_3_months.shape[0]
    coverage_ratio = actual_points / expected_points
    
    if coverage_ratio >= threshold:
        return True
    else:
        return False


def calculate_cal_factor(data):
    data['C_RH'] = data['humidity'].apply(lambda x: 1 if x < 65 else (0.0121212 * x) + 1 if x < 85 else (1 + ((0.2 / 1.65) / (-1 + 100 / min(x, 90)))))
    data['pm2_5_corrected'] = data['pm2_5'] / data['C_RH']
    station_average = float(data['pm2_5_corrected'].mean())
    pattern_average = float(data['pattern_pm2_5'].mean())
    calibration_factor = pattern_average / station_average
    return calibration_factor, station_average, pattern_average 

def get_cal_data(data):
    calibration_factor, station_average, pattern_average = calculate_cal_factor(data)
    date_start_cal = min(data['date_utc'])
    date_end_cal = max(data['date_utc'])
    date_start_use = date_end_cal + relativedelta(hours = 1)
    date_end_use = date_start_use + relativedelta(months = 1)
    cal_data = pd.DataFrame(
        {
            'station_id': data['station_id'].iloc[0],
            'date_start_cal': date_start_cal,
            'date_end_cal': date_end_cal,
            'station_mean': station_average,
            'pattern_mean': pattern_average,
            'date_start_use': date_start_use,
            'date_end_use': date_end_use,
            'calibration_factor': calibration_factor
        },
        index =[0]
    )
    return cal_data

@transformer
def transform(data, *args, **kwargs):
    klogger = kwargs.get('logger')

    exec_date = kwargs['execution_date'] 
    try:
        if data is None:
            klogger.exception('No data for calibration')
            return pd.DataFrame()

        if check_data_coverage(data):
            cal_data = get_cal_data(data)
            return cal_data
        else:
            klogger.exception(f"Not enough data to calibrate this month for station {data['station_id'].iloc[0]}")
            return pd.DataFrame()
    except Exception as e:
        klogger.exception(e)

    return pd.DataFrame()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'