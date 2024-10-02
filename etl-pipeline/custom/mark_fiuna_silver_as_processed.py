if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from pandas import DataFrame

@custom
def transform_custom(data, *args, **kwargs) -> DataFrame:
    klogger = kwargs.get('logger')
    
    try:
        if data.empty:
            raise Exception('Dataframe is empty')
        data = data[['silver_id', 'processed_to_gold']]
        data.rename(columns = {'silver_id': 'id'}, inplace = True)
        data['processed_to_gold'] = True
    except Exception as e:
        klogger.exception(e)
    
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
