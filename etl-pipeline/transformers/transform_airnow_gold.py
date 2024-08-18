from pytz import timezone

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def convert_to_local_time(time_utc):
    local_time = timezone('America/Asuncion')
    utc_minus_0 = timezone('UTC')
    time_utc = utc_minus_0.localize(time_utc)
    time_utc_local = time_utc.astimezone(local_time)
    return time_utc_local.replace(tzinfo=None)

def process_data(df):
    df['date_localtime'] = df['date_utc'].apply(convert_to_local_time)
    df.rename(columns = {'station_id':'station'}, inplace = True)
    df.drop(columns = ['date_utc'], inplace = True)
    return df

@transformer
def transform(data, *args, **kwargs):
    # Specify your transformation logic here
    processed_data = data.groupby('station_id').apply(process_data).reset_index(drop=True)

    return processed_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'