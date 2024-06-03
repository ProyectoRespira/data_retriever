from src.transform.utils import prepare_fiuna_records_for_insertion, prepare_meteostat_data_for_insertion
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_fiuna_data(fiuna_data):
    try:
        transformed_fiuna_data = prepare_fiuna_records_for_insertion(fiuna_data)
        return transformed_fiuna_data, True
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        return None, False
        
def transform_meteostat_data(meteostat_data):
    try:
        if meteostat_data is not None:
            transformed_meteostat_data = prepare_meteostat_data_for_insertion(meteostat_data)
            return transformed_meteostat_data, True
        else:
            return None, True
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        return None, False

