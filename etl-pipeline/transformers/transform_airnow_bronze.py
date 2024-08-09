from pytz import timezone
from datetime import datetime
import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(data, *args, **kwargs):
    # Specify your transformation logic here
    data['date_utc'] = pd.to_datetime(data['date_utc'], format = "%Y-%m-%dT%H:%M")
    #data['date_utc'] = data['date_utc'].dt.strftime("%Y-%m-%d %H:%M")
    # fill missing dates

    processed_data = pd.DataFrame()

    for station_id, group in data.groupby('station_id'):
        # change date format and fill datetime index
        group.set_index('date_utc', inplace = True)
        group.index = pd.to_datetime(group.index, format = "%Y-%m-%d %H:%M")
        group = group.resample('h').asfreq().reset_index()
        # replace nan or invalid readings with interpolation
        group['pm2_5'] = group['pm2_5'].replace(-999, np.nan)
        group['pm2_5'] = group['pm2_5'].interpolate()
        # make sure every entry has station_id
        group['station_id'] = station_id

        processed_data = pd.concat([processed_data, group])

    processed_data.reset_index(drop=True, inplace = True)

    print(processed_data.head())

    return processed_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'