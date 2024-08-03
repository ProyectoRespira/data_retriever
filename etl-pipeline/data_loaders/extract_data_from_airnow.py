import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
from dotenv import load_dotenv

@data_loader
def load_data_from_api(data, *args, **kwargs) -> pd.DataFrame:
    """
    Template for loading data from API
    """
    load_dotenv()

    url = "https://airnowapi.org/aq/data/" \
        + "?startdate=" + data["start_date"] \
        + "t" + data["start_hour_utc"] \
        + "&enddate=" + data["end_date"] \
        + "t" + data["end_hour_utc"] \
        + "&parameters=" + data["parameters"] \
        + "&bbox=" + data["bbox"] \
        + "&datatype=" + data["data_type"] \
        + "&format=" + data["format"] \
        + "&api_key=" + os.getenv('AIRNOW_API_KEY')


    response = requests.get(url)

    responses = response.json()

    df = pd.json_normalize(responses)
    df['station_id'] = data['station_id']

    print(df.info())

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'