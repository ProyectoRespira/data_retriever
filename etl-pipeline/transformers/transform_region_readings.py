if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

def fill_missing_data(df):
    df = df.sort_values(by=['station_id', 'date_utc'])  # Ensure data is sorted for filling
    df['pm2_5'] = df.groupby('station_id')['pm2_5'].fillna(method='ffill').fillna(method='bfill')  # Forward fill
    df['aqi_pm2_5'] = df.groupby('station_id')['aqi_pm2_5'].fillna(method='ffill').fillna(method='bfill')  # Forward fill
    df['aqi_level'] = df.groupby('station_id')['aqi_level'].fillna(method='ffill').fillna(method='bfill')  # Forward fill
    return df

# Helper function to calculate the metrics for each region group
def calc_metrics(group):

    metrics = {
        'date_utc': group['date_utc'].iloc[0],
        'region_id': group['region_id'].iloc[0],
        'pm2_5_region_avg': group['pm2_5'].mean(),
        'pm2_5_region_max': group['pm2_5'].max(),
        'pm2_5_region_skew': group['pm2_5'].skew(),
        'pm2_5_region_std': group['pm2_5'].std(),
        'aqi_region_avg': group['aqi_pm2_5'].mean(),
        'aqi_region_max': group['aqi_pm2_5'].max(),
        'aqi_region_skew': group['aqi_pm2_5'].skew(),
        'aqi_region_std': group['aqi_pm2_5'].std(),
        'level_region_max': group['aqi_level'].max(),
    }
    return pd.Series(metrics)

# Helper function to calculate metrics for each region and date_utc group
def calculate_region_metrics(df):
    df = fill_missing_data(df)
    grouped = df.groupby(['date_utc', 'region_id'])
    region_metrics = grouped.apply(calc_metrics).reset_index(drop=True)
    region_metrics.dropna(inplace=True)
    return region_metrics

@transformer
def transform(data, *args, **kwargs):

    transformed_data = calculate_region_metrics(data)

    return transformed_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'