
from src.models import StationsReadingsRaw
from src.time_utils import convert_to_local_time
import pandas as pd
import numpy as np
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# fiuna data
def validate_fiuna_data(fiuna_data):
    if not isinstance(fiuna_data, dict):
        raise ValueError("Input data must be a dictionary")

    for station_id, records in fiuna_data.items():
        if not isinstance(station_id, int):
            raise ValueError(f"Station ID must be an integer, got {type(station_id).__name__}")

        if not isinstance(records, list):
            raise ValueError(f"Records for station {station_id} must be a list, got {type(records).__name__}")

        for record in records:
            if not isinstance(record, dict):
                raise ValueError(f"Each record must be a dictionary, got {type(record).__name__}")

            if 'id' not in record:
                raise ValueError("Each record must contain an 'id' field")

            # Add other required keys if needed, for example:
            required_keys = ['id', 'fecha', 'hora', 'mp2_5', 'mp1', 'mp10', 'temperatura', 'humedad', 'presion', 'bateria']
            for key in required_keys:
                if key not in record:
                    raise ValueError(f"Record missing required key: {key}")

            # Add type checks for each field if needed
            if not isinstance(record['id'], int):
                raise ValueError(f"Record 'id' must be an integer, got {type(record['id']).__name__}")


def prepare_fiuna_records_for_insertion(fiuna_data):
    logging.info('Starting prepare_records_for_insertion...')

    all_records = []

    for station_id, records in fiuna_data.items():
        df = pd.DataFrame(records)
        df['station_id'] = station_id
        df.rename(columns={'id': 'measurement_id'}, inplace=True)
        all_records.append(df)

    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(all_records, ignore_index=True)

    # Convert the DataFrame back to a list of dictionaries for insertion
    records_list = combined_df.to_dict(orient='records')

    # Convert to a list of StationsReadingsRaw objects
    prepared_records_per_station = [
        StationsReadingsRaw(**record) for record in records_list
    ]

    # Group by station_id
    prepared_records = {}
    for record in prepared_records_per_station:
        if record.station_id not in prepared_records:
            prepared_records[record.station_id] = []
        prepared_records[record.station_id].append(record)

    return prepared_records


# meteostat data

def prepare_meteostat_data_for_insertion(meteostat_data_dict):
    transformed_data_list = []
    for station_id, meteostat_data in meteostat_data_dict.items():
        meteostat_data.index = meteostat_data.index.map(convert_to_local_time)
        meteo_features = ['temp', 'rhum', 'pres', 'wspd', 'wdir']
        meteostat_data = meteostat_data[meteo_features]
        meteostat_data.rename(columns={
            'temp': 'temperature',
            'rhum': 'humidity',
            'pres': 'pressure',
            'wspd': 'wind_speed',
            'wdir': 'wind_dir'
        }, inplace=True)
        meteostat_data['weather_station'] = station_id
        meteostat_data['wind_dir_cos'] = np.cos(2 * np.pi * meteostat_data.wind_dir / 360)
        meteostat_data['wind_dir_sin'] = np.sin(2 * np.pi * meteostat_data.wind_dir / 360)
        meteostat_data.drop('wind_dir', axis=1, inplace=True)
        meteostat_data['date'] = meteostat_data.index
        meteostat_data.round(2)
        transformed_data_list.append(meteostat_data)
    if len(transformed_data_list) > 1:
        transformed_data = pd.concat(transformed_data)
    else:
        transformed_data = transformed_data_list[0]
        print(transformed_data.head())
    return transformed_data

# airnow data

def prepare_airnow_data_for_insertion(response_dict):
    try:
        transformed_data = []
        for station_id, data in response_dict.items():
            for entry in data:
                local_date = convert_to_local_time(datetime.strptime(entry['UTC'],"%Y-%m-%dT%H:%M"))
                transformed_data_entry = {
                        'station': station_id,
                        'date': local_date,
                        'pm2_5': entry['Value']
                    }
                transformed_data.append(transformed_data_entry)
    except KeyError as e:
        raise ValueError(f'KeyError in data transformation: {e}')
    except ValueError as e:
        raise ValueError(f'ValueError in data transformation: {e}')
    
    return transformed_data
