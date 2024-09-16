from meteostat import Point, Hourly
from dateutil.relativedelta import relativedelta 
import pandas as pd


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(data, *args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    execution_type = kwargs['execution_type']
    # Specify your data loading logic here
    if execution_type == 'incremental':
        end_date = kwargs['execution_date']
        start_date = end_date - relativedelta(hours = 1)
    elif execution_type == 'backfill_year':
        end_date = kwargs['execution_date']
        start_date = end_date - relativedelta(years = 1)
    elif execution_type == 'backfill_day':
        end_date = kwargs['execution_date']
        start_date = end_date - relativedelta(days = 1)
    elif execution_type == 'backfill_month':
        end_date = kwargs['execution_date']
        start_date = end_date - relativedelta(months = 1)
        
    new_data = []
    
    for i in data.index:
        coordinates = Point(data['latitude'].iloc[i], 
                            data['longitude'].iloc[i],
                            101)
        df_aux = Hourly(coordinates, start_date, end_date).fetch()
        df_aux['station_id'] = data['station_id'].iloc[i]
        new_data.append(df_aux)

    if len(new_data) == 1:
        df = new_data[0]
    else:
        df = pd.concat(new_data)

    df['date_utc'] = pd.to_datetime(df.index)
    df['date_utc'] = df['date_utc'].dt.tz_localize('UTC')
    df.rename(columns = {'temp': 'temperature'}, inplace = True)
    df.reset_index(drop=True)
    
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'