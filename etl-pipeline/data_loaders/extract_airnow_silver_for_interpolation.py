from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_postgres(data, *args, **kwargs):
    """
    Template for loading data from a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.
    
    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    klogger = kwargs.get('logger')

    if isinstance(data, list):
        data = pd.DataFrame(data)
        klogger.info('')
    # Calculate the dates for one hour before and after the data range
    min_date = data['date_utc'].min() 
    max_date = data['date_utc'].max() 
    
    # Convert station IDs to a list of native Python int types
    station_ids = [int(station_id) for station_id in data['station_id'].unique()]
    
    # SQL query with placeholders for parameter binding
    query = '''
    WITH before_min_date AS (
        SELECT 
            silver.measurement_id,
            silver.station_id,
            silver.date_utc,
            silver.pm2_5
        FROM airnow_readings_silver silver
        WHERE silver.date_utc < %(min_date)s
        AND silver.date_utc >= %(min_date)s - INTERVAL '2 hours'
        AND silver.station_id = ANY(%(station_ids)s)
        ORDER BY silver.date_utc DESC
        LIMIT 1
        ),
    after_max_date AS (
        SELECT 
            silver.measurement_id,
            silver.station_id,
            silver.date_utc,
            silver.pm2_5
        FROM airnow_readings_silver silver
        WHERE silver.date_utc > %(max_date)s
        AND silver.date_utc <= %(max_date)s + INTERVAL '2 hours'
        AND silver.station_id = ANY(%(station_ids)s)
        ORDER BY silver.date_utc ASC
        LIMIT 1
    )
    SELECT * FROM before_min_date
    UNION ALL
    SELECT * FROM after_max_date;
    '''
    
    # Set up the configuration for PostgreSQL connection
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Execute the query using the configuration and pass parameters using 'params'
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return loader.load(query, params={
            'min_date': min_date,
            'max_date': max_date,
            'station_ids': station_ids  # Ensure IDs are native Python int
        })


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'