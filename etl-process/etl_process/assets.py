
from typing import List

from dagster import asset, AssetExecutionContext


@asset(
    description="Extract data from source",
    compute_kind="python",
    group_name="extract"
)
def select_data(context: AssetExecutionContext):
    context.log.info("Extracting data from source")
    return "step 1"


@asset(
    description="Transform data from source",
    compute_kind="python",
    group_name="transform"
)
def transform_data(context: AssetExecutionContext, select_data):
    context.log.info("Transforming data from source")
    return "step 2"


@asset(
    description="Load data to destination",
    compute_kind="python",
    group_name="load"
)
def load_data(context: AssetExecutionContext, transform_data):
    context.log.info("Loading data to destination")
    return "step 3"