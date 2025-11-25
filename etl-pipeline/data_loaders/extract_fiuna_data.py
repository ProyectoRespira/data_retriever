from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path
import time
import pandas as pd  

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_mysql(data, *args, **kwargs):
    """
    Loads data from a MySQL database with a timeout.
    Adds debug info to log query results (row count, columns, sample rows).
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
                    df = loader.load(query)

                    if df is None or df.empty:
                        klogger.warning(f"No data returned for station {station_id}. Query: {query}")
                        return pd.DataFrame()  # empty dataframe instead of None
                    else:
                        klogger.info(f"✅ Loaded {len(df)} rows for station {station_id}.")
                        klogger.info(f"Columns: {list(df.columns)}")
                        klogger.info(f"Sample rows:\n{df.head(3)}")
                        return df

                except ConnectionError as e:
                    klogger.warning(f"Retrying due to connection issue: {e}")
            raise TimeoutError("Data loading timed out.")

    except TimeoutError as e:
        klogger.error(f"Data loading timed out after {timeout_seconds} seconds.")
        return pd.DataFrame()  # 👈 Empty dataframe instead of None

    except Exception as e:
        klogger.exception(f"An error occurred: {e}")
        return pd.DataFrame()  
