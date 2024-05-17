from src.mirror import retrieve_data
from src.meteostat_data import fill_meteostat_data
from src.transform_raw_data import fill_station_readings

def main():
    
    meteostat_data_status = retrieve_data() # retrieve data from FIUNA
    if meteostat_data_status:
        fill_data_status = fill_meteostat_data() # retrieve data from Meteostat
        if fill_data_status:
            fill_station_status = fill_station_readings() # transform into StationReadings data - for usage in frontend
            if fill_station_status:
                return "Station readings filled successfully"
            else: 
                return "Error: Fillingg station readings failed"
        else:
            return "Error: Filling meteostat data failed"
    else:
        return "Error: Retrieving data from FIUNA failed"

if __name__ == "__main__":
    main()