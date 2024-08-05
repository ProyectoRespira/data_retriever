from pytz import timezone
from datetime from datetime import datetime

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def convert_to_local_time(time_utc):
    local_time = timezone('America/Asuncion')
    utc_minus_0 = timezone('UTC')
    time_utc = utc_minus_0.localize(time_utc)
    time_utc_local = time_utc.astimezone(local_time)
    return time_utc_local.replace(tzinfo=None)


@custom
def transform_airnow_raw_readings(data, *args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here
    

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
