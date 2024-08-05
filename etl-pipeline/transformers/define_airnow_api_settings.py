if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
from typing import Dict, List

@transformer
def transform(data, *args, **kwargs) -> Dict:
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # # Specify your transformation logic here
    # data.set_index('station_id', inplace = True, drop = True)

    data['end_date_utc'] = datetime.now(timezone('UTC')) 
    data['end_date'] = data['end_date_utc'].dt.strftime('%Y-%m-%d')
    data['end_hour_utc'] = data['end_date_utc'].dt.strftime('%H')
    
    
    #data['start_date_utc'] = data['start_date_utc'].fillna(value = datetime(2024,7,1,0,0,0,0)).infer_objects(copy=False)
    #data['start_date_utc] = data['start_date_utc'] + timedelta(hours = 1)
    data['start_date_utc'] = data['end_date_utc'] - timedelta(hours = 1)
    data['start_date'] = data['start_date_utc'].dt.strftime('%Y-%m-%d')
    data['start_hour_utc'] = data['start_date_utc'].dt.strftime('%H')

    data.drop('end_date_utc', axis = 1, inplace = True)
    data.drop('start_date_utc', axis = 1, inplace = True)
    

    data['parameters'] = 'pm25'
    data['data_type'] = 'c'
    data['format']='application/json'
    data['verbose'] = 1
    data['includerawconcentrations'] = 1
    
    data = data.to_dict(orient = 'records')
    # metadata = []
    # for row in data:
    #     metadata.append(dict(block_uuid=f"for_station_{row['station_id']}"))

    return data


# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'