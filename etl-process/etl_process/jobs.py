from dagster import define_asset_job, ScheduleDefinition
from etl_process.assets import select_data, transform_data, load_data

run_pipeline_job = define_asset_job("run_pipeline", [select_data, transform_data, load_data])