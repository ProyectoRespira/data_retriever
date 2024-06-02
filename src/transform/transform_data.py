from src.transform.utils import prepare_fiuna_records_for_insertion
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_fiuna_data(fiuna_data):
    try:
        transformed_fiuna_data = prepare_fiuna_records_for_insertion(fiuna_data)
        return transformed_fiuna_data, True
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        return None, False
        
