
from src.models import StationsReadingsRaw
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

    # Create an empty DataFrame to hold all the records
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