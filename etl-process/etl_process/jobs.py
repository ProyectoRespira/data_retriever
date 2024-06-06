from dagster import job, define_asset_job, ScheduleDefinition

from etl_process.assets import select_data, transform_data, load_data


@job
def run_pipeline():
    select_data
    transform_data
    load_data

run_pipeline_job = define_asset_job("run_pipeline", [select_data, transform_data, load_data])
daily_job_run = ScheduleDefinition(job=run_pipeline_job, cron_schedule="0 0 * * *")