from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_mysql(data,*args, **kwargs):
    """
    Template for loading data from a MySQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#mysql
    """

    station_id = data['station_id']
    table_name = f'Estacion{ station_id }'
    last_measurement_id = data['last_measurement_id']

    query = f''' SELECT *, {station_id} AS station_id
                FROM {table_name} 
                WHERE ID > {last_measurement_id}'''  

    config_path = path.join(get_repo_path(), 'io_config.yaml') 
    config_profile = 'default' 

    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return loader.load(query)

        


# @test 
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'