import pandas as pd
import json
from glob import glob
from datetime import datetime
import os
from darts import TimeSeries
from darts.models import LightGBMModel

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def prepare_data(data):
    data.drop(columns=['station', 'inference_run'], inplace=True)
    run_date = pd.to_datetime(data['run_date'].iloc[0], utc=True).tz_convert(None)
    data['date_utc'] = pd.to_datetime(data['date_utc'], utc=True).dt.tz_convert(None)
    data = data.drop_duplicates(subset='date_utc').sort_values(by='date_utc')
    min_date_utc = data['date_utc'].min()
    
    if pd.api.types.is_datetime64tz_dtype(min_date_utc):
        min_date_utc = min_date_utc.tz_convert(None)
    
    full_range = pd.date_range(start=min_date_utc, end=run_date, freq='H')

    data = data.set_index('date_utc').reindex(full_range).rename_axis('date_utc').reset_index()

    data.fillna(method='ffill', inplace=True) 
    data.fillna(method='bfill', inplace=True)  
    data = data[data['date_utc'] <= run_date]
    data.reset_index(drop=True, inplace=True)

    if 'run_date' in data.columns:
        data.drop(columns=['run_date'], inplace=True)
    return data

def get_latest_model_path(model_dir, model_name, klogger):
    try:
        all_files = os.listdir(model_dir)
        model_files = []
        for file_name in all_files:
            file_name_no_ext = os.path.splitext(file_name)[0]
            try:
                # Format: <YYYY-MM-DD_vX.X.X_model-6h/12h.pkl>
                date_part, version_part, model_part = file_name_no_ext.split('_')
                datetime.strptime(date_part, "%Y-%m-%d")

                if model_part == model_name and version_part.startswith('v'):
                    version_numbers = version_part[1:].split('.')
                    if all(num.isdigit() for num in version_numbers):
                        model_files.append(file_name)
            except (ValueError, IndexError):
                klogger.warning(f'Unvalid filename: {file_name}')
                continue

        if not model_files:
            klogger.exception(f"No valid models found for {model_name} in directory {model_dir}")
            return None
        # Latest model first in array:
        model_files.sort(reverse=True)
        return os.path.join(model_dir, model_files[0])
    except Exception as e:
        klogger.exception(f"An error occurred while getting latest model path: {e}")
        return None


def load_models(klogger, model_dir='etl-pipeline/models/'):
    try:
        model_12h_path = get_latest_model_path(model_dir, 'model-12h', klogger)
        model_6h_path = get_latest_model_path(model_dir, 'model-6h', klogger)

        model_12h = LightGBMModel.load(model_12h_path)
        model_6h = LightGBMModel.load(model_6h_path)

        return model_6h, model_12h
    except Exception as e:
        klogger.exception(f'An error occurred while loading models: {e}')

def predict_aqi(data, model, output_length, klogger):
    try:
        target = 'aqi_pm2_5'
        covariates = list(data.columns.drop(['date_utc']))
        ts = TimeSeries.from_dataframe(data, time_col='date_utc', value_cols=[target] + covariates, freq='h')
        target_data = ts[target]
        covariates_data = ts[covariates]

        y_pred = model.predict(output_length, series=target_data, past_covariates=covariates_data)

        y_pred_series = y_pred.pd_series().round(0) 
        result = [
            {
                "timestamp": timestamp.isoformat(), 
                "value": int(value) 
            }
            for timestamp, value in y_pred_series.items()
        ]
        return result
    except Exception as e:
        klogger.exception(f'An error occurred while predicting aqi: {e}')

@custom
def transform_custom(data, *args, **kwargs):
    klogger = kwargs.get('logger')
    try:
        station = data['station'].iloc[0]
        inference_run = data['inference_run'].iloc[0]

        pred_data = prepare_data(data)

        aqi_df = pred_data[['date_utc','aqi_pm2_5']] 
        aqi_json_list = aqi_df.apply(lambda row: {"timestamp": row['date_utc'].isoformat(), "value": int(row['aqi_pm2_5'])}, axis=1).tolist()
        aqi_json = json.dumps(aqi_json_list, indent=4)

        model_6h, model_12h = load_models(klogger=klogger)
        forecast_12h = predict_aqi(pred_data, model_12h, 
                                    output_length=12, klogger=klogger)
        forecast_6h = predict_aqi(pred_data, model_6h, 
                                    output_length=6, klogger=klogger)
        result_df = pd.DataFrame({
            'inference_run': [inference_run],
            'station': [station],
            'aqi_input': [aqi_json],
            'forecasts_6h': [forecast_6h],
            'forecasts_12h': [forecast_12h]
        })
        return result_df
    except Exception as e:
        klogger.exception(e)

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'