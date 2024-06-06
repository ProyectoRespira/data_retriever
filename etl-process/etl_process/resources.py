from dagster import ConfigurableResource


class SomeCredentials(ConfigurableResource):
    user: str
    password: str
    host: str
    