from src.transform.utils import prepare_fiuna_records_for_insertion, prepare_meteostat_data_for_insertion, prepare_airnow_data_for_insertion
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

def transform_airnow_data(airnow_data):
    try:
        if airnow_data is not None:
            transformed_airnow_data = prepare_airnow_data_for_insertion(airnow_data)
            return transformed_airnow_data, True
        else:
            logging.warning("No data provided for transformation.")
            return None, True
    except KeyError as e:
        logging.error(f"KeyError: {e}. Check the structure of the input data.")
        return None, False
    except ValueError as e:
        logging.error(f"ValueError: {e}. Error in parsing date or value.")
        return None, False
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None, False

