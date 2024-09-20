if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

# Helper function to calculate the metrics for each region group
def calc_metrics(group):

    metrics = {
        'date_utc': group['date_utc'].iloc[0],
        'region': group['region'].iloc[0],
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
    grouped = df.groupby(['date_utc', 'region'])
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