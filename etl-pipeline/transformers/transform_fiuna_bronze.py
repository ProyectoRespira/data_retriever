if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    klogger = kwargs.get('logger')

    data.columns = data.columns.str.lower()
    data.rename(columns={'id':'measurement_id'}, inplace = True)
    
    station_id = data['station_id'].iloc[0]  # Get the station_id (only one value)
    number_of_readings = len(data)  # Count the number of rows in the DataFrame

    klogger.info(f"Number of new readings for station ID {station_id}: {number_of_readings}")
    
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'