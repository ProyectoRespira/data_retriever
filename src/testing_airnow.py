import os
import sys
from datetime import datetime
from os.path import expanduser
import requests

# https://docs.airnowapi.org/Data/docs

def main():

    # API parameters
    options = {}
    options["url"] = "https://airnowapi.org/aq/data/"
    options["start_date"] = "2024-05-20"
    options["start_hour_utc"] = "00"
    options["end_date"] = "2024-05-20"
    options["end_hour_utc"] = "10"
    options["parameters"] = "pm25"
    options["bbox"] = "-57.725,-25.384,-57.500,-25.214"
    options["data_type"] = "c" # options: a (AQI), b (concentrations & AQI), c (concentrations)
    options["format"] = "application/json" # options: 'text/csv', 'application/json', 'application/vnd.google-earth.kml', 'application/xml'
    #options["ext"] = "csv" 
    options["api_key"] = "D4CD30B3-FFEB-4DB1-90C6-CC669003ABA8"
    options["verbose"] = 1
    options["includerawconcentrations"] = 1

    # API request URL
    REQUEST_URL = options["url"] \
                  + "?startdate=" + options["start_date"] \
                  + "t" + options["start_hour_utc"] \
                  + "&enddate=" + options["end_date"] \
                  + "t" + options["end_hour_utc"] \
                  + "&parameters=" + options["parameters"] \
                  + "&bbox=" + options["bbox"] \
                  + "&datatype=" + options["data_type"] \
                  + "&format=" + options["format"] \
                  + "&api_key=" + options["api_key"]

    try:
        # Request AirNowAPI data
        print("Requesting AirNowAPI data...")

        # User's home directory.
        # home_dir = expanduser("~/data_retriever/")
        # download_file_name = "AirNowAPI" + datetime.now().strftime("_%Y%m%d%H%M%S." + options["ext"])
        # download_file = os.path.join(home_dir, download_file_name)

        # Perform the AirNow API data request
        response = requests.get(REQUEST_URL)
        response.raise_for_status()  # Check if the request was successful
        print(response.content)

        # Write the data to a file
        # with open(download_file, 'wb') as file:
        #     file.write(response.content)

        # # Download complete
        # print(f"Download URL: {REQUEST_URL}")
        # print(f"Download File: {download_file}")

    except Exception as e:
        print(f"Unable to perform AirNowAPI request. {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()