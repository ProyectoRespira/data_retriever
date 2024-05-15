from src.mirror import retrieve_data
from src.meteostat_data import fill_meteostat_data
from src.transform_raw_data import fill_station_readings

def main():
    
    retrieve_data()
    fill_meteostat_data()
    fill_station_readings()

if __name__ == "__main__":
    main()