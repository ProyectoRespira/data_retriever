from src.mirror import retrieve_data
from src.meteostat_data import fill_meteostat_data
from src.transform_raw_data import fill_station_readings
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():

    retrieve_data_status = retrieve_data()
    if retrieve_data_status is None:
        return "Error: Retrieving data from FIUNA failed"
    
    meteostat_data_status = fill_meteostat_data()
    if meteostat_data_status is None:
        return "Error: Filling meteostat data failed"
    
    station_readings_status = fill_station_readings()
    if station_readings_status is None:
        return "Error: Filling station readings failed"
    
    return "Station readings filled successfully"

if __name__ == "__main__":
    message = main()
    logging.info(message)
