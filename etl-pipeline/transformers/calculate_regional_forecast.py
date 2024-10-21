import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    klogger = kwargs.get('logger')
    try:
        if data.empty:
            klogger.exception('No available forecasts')
        df_exploded = data.explode('forecasts_12h').reset_index(drop=True)
        df_exploded[['timestamp', 'value']] = pd.json_normalize(df_exploded['forecasts_12h'])
        df_transformed = df_exploded.drop(columns=['forecasts_12h'])
        df_transformed['timestamp'] = pd.to_datetime(df_transformed['timestamp'], utc=True)

        df_grouped = (
            df_transformed.groupby(['timestamp'], as_index=False)['value']
            .mean().round()
            .rename(columns={'value': 'forecast_avg'})
        )
        return df_grouped
    except Exception as e:
        klogger.exception(f'An error occurred: {e}')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'