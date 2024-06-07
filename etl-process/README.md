# etl_process

This is a Dagster project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).


## Getting started
Install project
```bash
pip install -e ".[dev]"
```

Start the Dagster UI web server:
```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.


## Dagster Usage

The core of dagster is generating software-defined assets. Assets can be generated in two ways:


### Assets
Define it as an [`@asset`](https://docs.dagster.io/concepts/assets/software-defined-assets):
```python
from typing import List
from dagster import asset, AssetExecutionContext, Output
from project.resources import SomeResource

@asset(
    description='Asset description, similar to docstring',
    compute_kind='python',
    group_name='extract'
)
def asset_name(context: AssetExecutionContext, credentials: SomeResource) -> Output[List[str]]:
    # do some computation
    result = computation()

    context.log.info(f"Some info to log: {result}")

    # return output with metadata
    return Output(
        result, 
        metadata={
            "n_result": len(result),
            "preview": result[:5],
        }
    )
```

### Graph Asset
Define it a [`@graph_asset`](https://docs.dagster.io/concepts/assets/graph-backed-assets)
```python
@op()
def fetch_file() -> str:
    return "my_file"

@op
def store_file(result: str) -> str:
    return result

@graph_asset(
    description='Using @graph_asset decorator',
    group_name='extract'
)
def generate_asset_from_graph():
    return store_file(fetch_file())
```


### Resources
If you need a config for an asset, use a `ConfigurableResource`:
```python
# resources.py
from dagster import ConfigurableResource

class SomeCredentials(ConfigurableResource):
    user: str
    password: str
    host: str
```
```python
# assets.py
from project.resources import SomeCredentials

@asset()
def asset_name(context: AssetExecutionContext, credentials: SomeCredentials):
    context.log.info(f"Current user: {credentials.user}")
```


### Jobs
Organize how assets should be materialized with `jobs`, can also use `schedules` and `sensors` to automate:
```python
# jobs.py
from dagster import job, define_asset_job, AssetSelection
from project.assets import asset_alpha, asset_beta

# a job that materializes all assets in the current definition
materialize_all_job = define_asset_job("update_all_job", selection=AssetSelection.all())

# job that materializes asset_alpha and asset_beta (infers dependency, if it exists)
asset_alpha_job = define_asset_job("alpha_job", selection=[asset_alpha, asset_beta])

# TODO: add scheduler and sensor information
```

### Definitions
Add newly created assets to project definitions so that they can be discovered and displayed in the UI:
```python
# __init__.py
from dagster import Definitions, EnvVar
from project import assets, jobs, resources

defs = Definitions(
    assets=[
        load_assets_from_modules([assets])
    ],
    jobs=[
        jobs.run_pipeline_job
    ],
    resources={
        "credentials": resources.SomeCredential(
            user=EnvVar('USER'),
            password=EnvVar('PASSWORD'),
            host=EnvVar('HOST')
        )
    }
)
```

