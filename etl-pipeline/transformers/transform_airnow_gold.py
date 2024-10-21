from pytz import timezone, utc
import pandas as pd
import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def convert_to_local_time(time_utc):
    local_time_zone = timezone('America/Asuncion')
    # Convert to local time zone
    time_utc_local = time_utc.astimezone(local_time_zone)
    return time_utc_local

def process_data(df):
    # Ensure 'date_utc' is a datetime column and timezone-aware
    df['date_utc'] = pd.to_datetime(df['date_utc'], utc=True)  # Parse dates with UTC timezone
    df['date_utc'] = df['date_utc'].apply(convert_to_local_time)
    
    #df.drop(columns=['date_utc'], inplace=True)
    # Sort the DataFrame by 'date_utc'
    df = df.sort_values(by='date_utc')
    return df

@transformer
def transform(data, *args, **kwargs):
    # Apply process_data to each group and then sort by 'date_utc'
    processed_data = data.groupby('station_id').apply(process_data).reset_index(drop=True)
    return processed_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'