
from typing import List
from dagster import asset, AssetExecutionContext
from etl_process.resources import SomeCredentials


@asset(
    description="Extract data from source",
    compute_kind="python",
    group_name="extract"
)
def select_data(context: AssetExecutionContext, credentials: SomeCredentials) -> str:
    context.log.info("Extracting data from source")
    context.log.info(f"User: {credentials.user} | Password: {credentials.password} | Host: {credentials.host}")
    return "step 1"


@asset(
    description="Transform data from source",
    compute_kind="python",
    group_name="transform"
)
def transform_data(context: AssetExecutionContext, select_data: str) -> str:
    context.log.info("Transforming data from source")
    return "step 2"


@asset(
    description="Load data to destination",
    compute_kind="python",
    group_name="load"
)
def load_data(context: AssetExecutionContext, transform_data: str) -> str:
    context.log.info("Loading data to destination")
    return "step 3"