if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from pandas import DataFrame

@transformer
def transform(data, *args, **kwargs) -> DataFrame:
    
    klogger = kwargs.get('logger')
    
    try:
        if df.empty:
            raise Exception('Dataframe is empty')
        data = data[['silver_id', 'date_localtime']]
    except Exception as e:
        klogger.exception(e)
    
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'