if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    klogger = kwargs.get('logger')

    try:
        
        if data is None or data.empty:
            klogger.warning("⚠️ Empty DataFrame from transform. No new readings for this station.")
            return data

        data.columns = data.columns.str.lower()
        data.rename(columns={'id': 'measurement_id'}, inplace=True)
        
        station_id = data['station_id'].iloc[0]
        number_of_readings = len(data)

        klogger.info(f"✅ New readings for station {station_id}: {number_of_readings}")
        return data

    except Exception as e:
        klogger.exception(e)
        return data
    

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
