from dagster import Definitions, load_assets_from_modules
from etl_process import assets, jobs, resources


defs = Definitions(
    assets=[
        *load_assets_from_modules([assets]), 
    ],
    jobs=[
        jobs.run_pipeline_job
    ],
    resources={
        "credentials": resources.SomeCredentials(
            user="user",
            password="password",
            host="host",
        )
    }
)
