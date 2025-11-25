from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Export data to PostgreSQL only if DataFrame has data.
    """
    klogger = kwargs.get('logger')
    schema_name = 'public'
    table_name = 'station_readings_bronze'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    if df is None or df.empty or df.columns.size == 0:
        klogger.warning("⚠️ Skipping export: DataFrame is empty or missing columns.")
        return

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        klogger.info(f"✅ Exporting {len(df)} rows to {schema_name}.{table_name} ...")
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='append',
        )
        klogger.info("✅ Export completed successfully.")
