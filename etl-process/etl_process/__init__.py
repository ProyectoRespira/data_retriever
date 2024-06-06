from dagster import Definitions, load_assets_from_modules

from etl_process import assets, jobs, resources


all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[jobs.run_pipeline],
    resources={
        "credentials": resources.SomeCredentials(
            user="user",
            password="password",
            host="host",
        )
    }
)
