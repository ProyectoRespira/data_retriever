if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd


def fill_missing_data(df):
    df = df.sort_values(by=['station_id', 'date_utc']).copy()
    for col in ['pm2_5', 'aqi_pm2_5', 'aqi_level']:
        df[col] = df.groupby('station_id')[col].transform(lambda x: x.ffill().bfill())
    return df



def calc_metrics(group):
    if group.empty:
        return pd.Series()

    metrics = {
        'date_utc': group['date_utc'].iloc[0],
        'region_id': group['region_id'].iloc[0],
        'pm2_5_region_avg': group['pm2_5'].mean(),
        'pm2_5_region_max': group['pm2_5'].max(),
        'pm2_5_region_skew': group['pm2_5'].skew() or 0,
        'pm2_5_region_std': group['pm2_5'].std() or 0,
        'aqi_region_avg': group['aqi_pm2_5'].mean(),
        'aqi_region_max': group['aqi_pm2_5'].max(),
        'aqi_region_skew': group['aqi_pm2_5'].skew() or 0,
        'aqi_region_std': group['aqi_pm2_5'].std() or 0,
        'level_region_max': group['aqi_level'].max(),
    }

    # Replace Nan with 0
    return pd.Series({k: (0 if pd.isna(v) else v) for k, v in metrics.items()})


def calculate_region_metrics(df):
    print(df)
    df = fill_missing_data(df)

    grouped = df.groupby(['date_utc', 'region_id'])
    region_metrics = grouped.apply(calc_metrics).reset_index(drop=True)

    region_metrics = region_metrics.fillna(0)
    print(f"✅ Calculated {len(region_metrics)} rows of region metrics.")
    print(region_metrics.head())
    return region_metrics


@transformer
def transform(data, *args, **kwargs):
    klogger = kwargs.get('logger')
    transformed_data = calculate_region_metrics(data)
    klogger.info(f"✅ Finished region aggregation. Rows: {len(transformed_data)}")
    return transformed_data


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
