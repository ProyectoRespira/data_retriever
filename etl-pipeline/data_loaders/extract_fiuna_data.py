from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path
import time

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_mysql(data, *args, **kwargs):
    """
    Template for loading data from a MySQL database with a timeout.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#mysql
    """
    klogger = kwargs.get('logger')
    timeout_seconds = 10  # Set the timeout duration
    start_time = time.time()

    try:
        station_id = data['station_id']
        table_name = f'Estacion{station_id}'
        last_measurement_id = data['last_measurement_id']

        query = f''' SELECT *, {station_id} AS station_id
                     FROM {table_name} 
                     WHERE ID > {last_measurement_id}'''  

        config_path = path.join(get_repo_path(), 'io_config.yaml') 
        config_profile = 'default'

        with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            while time.time() - start_time < timeout_seconds:
                try:
                    return loader.load(query)
                except ConnectionError as e:
                    klogger.warning(f"Retrying due to connection issue: {e}")
            raise TimeoutError("Data loading timed out.")
    except TimeoutError as e:
        klogger.error(f"Data loading timed out after {timeout_seconds} seconds.")
        return None  # Return None on timeout
    except Exception as e:
        klogger.exception(f"An error occurred: {e}")
        return None  # Return None on other exceptions


        


# @test 
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'