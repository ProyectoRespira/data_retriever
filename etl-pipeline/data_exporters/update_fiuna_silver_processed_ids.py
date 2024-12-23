from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(data: DataFrame, data_2, **kwargs) -> None:

    klogger = kwargs.get('logger')

    schema_name = 'public'  # Specify the name of the schema to export data to
    table_name = 'station_readings_silver'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    try:
        if data.empty:
            klogger.exception('Dataframe is empty')

        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            loader.export(
                data,
                schema_name,
                table_name,
                index=False,  # Specifies whether to include index in exported table
                if_exists='append',  
                unique_conflict_method = 'UPDATE',
                unique_constraints = ['id']
            )
    except Exception as e:
        klogger.exception(e)