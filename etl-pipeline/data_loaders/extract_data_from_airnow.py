import io
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from typing import Any, Dict
from mage_ai.data_preparation.shared.secrets import get_secret_value
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_api(data, *args, **kwargs) -> Dict[str, Any]:
    """
    Template for loading data from API
    """
    kwarg_logger = kwargs.get('logger')
    execution_type = kwargs['execution_type']

    kwarg_logger.info(type(data))

    if execution_type == 'incremental':
        end_date_utc = kwargs['execution_date'] # now
        start_date_utc = end_date_utc 
    elif execution_type == 'backfill_year':
        start_date_utc = kwargs['execution_date']
        end_date_utc = start_date_utc + relativedelta(years=1)

    start_date = start_date_utc.strftime('%Y-%m-%d')
    start_hour = start_date_utc.strftime('%H')
    
    end_date = end_date_utc.strftime('%Y-%m-%d')
    end_hour = end_date_utc.strftime('%H')

    params = {
        "startdate": f"{start_date}t{start_hour}",
        "enddate": f"{end_date}t{end_hour}",
        "parameters": "pm25",
        "bbox": data["bbox"],
        "datatype": "c",
        "format": "application/json",
        "api_key": get_secret_value('AIRNOW_API_KEY'),
    }

    url = "https://airnowapi.org/aq/data/"

    try:
        response = requests.get(url, params=params, timeout=120)
    except E:
        logging.error(f"An error occurred during API request: {E}")
        return None

    responses = response.json()

    df = pd.json_normalize(responses)

    df['station_id'] = data['station_id']
    df['station_id'] = df['station_id'].ffill().bfill().astype(int)

    df.rename(columns = {'UTC': 'date_utc', 'Value': 'pm2_5'}, inplace = True)
    df.drop(columns = ['Latitude', 'Longitude', 'Parameter', 'Unit'], axis = 1, inplace = True)

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'