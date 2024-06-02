from src.initialize_db import create_postgres_tables
from src.extract.extract_data import extract_fiuna_data
from src.transform.transform_data import transform_fiuna_data
from src.load.load_data import load_station_readings_raw
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    create_postgres_tables()
    
    fiuna_data, status = extract_fiuna_data()
    if status is False:
        return 'Error: Extracting data from FIUNA failed'
    
    transformed_fiuna_data, status = transform_fiuna_data(fiuna_data)
    if status is False:
        return 'Error: Transforming data from FIUNA failed'
    
    load_status = load_station_readings_raw(transformed_fiuna_data)
    if load_status is False: 
        return 'Error: Loading data to StationReadingsRaw failed'
    
    return 'Success: Data from FIUNA loaded correctly'
    

if __name__ == "__main__":
    message = main()
    logging.info(message)