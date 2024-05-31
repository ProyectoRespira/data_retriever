import os
import sys
from datetime import datetime
import requests
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the SQLAlchemy base
Base = declarative_base()

# Define the database model for the air quality data
class USAirQualityReading(Base):
    __tablename__ = 'us_air_quality_readings'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date_utc = Column(DateTime)
    parameter = Column(String)
    value = Column(Float)
    latitude = Column(Float)
    longitude = Column(Float)
    unit = Column(String)

def main():

    # API parameters
    options = {
        "url": "https://airnowapi.org/aq/data/",
        "start_date": "2014-09-23",
        "start_hour_utc": "22",
        "end_date": "2014-09-23",
        "end_hour_utc": "23",
        "parameters": "o3,pm25",
        "bbox": "-90.806632,24.634217,-71.119132,45.910790",
        "data_type": "a",
        "format": "application/vnd.google-earth.kml",
        "ext": "kml",
        "api_key": "D4CD30B3-FFEB-4DB1-90C6-CC669003ABA8"
    }

    # API request URL
    REQUEST_URL = (
        f"{options['url']}?startdate={options['start_date']}t{options['start_hour_utc']}&"
        f"enddate={options['end_date']}t{options['end_hour_utc']}&parameters={options['parameters']}&"
        f"bbox={options['bbox']}&datatype={options['data_type']}&format={options['format']}&api_key={options['api_key']}"
    )

    try:
        # Request AirNowAPI data
        print("Requesting AirNowAPI data...")
        response = requests.get(REQUEST_URL)
        response.raise_for_status()
        kml_data = response.content

        # Parse the KML data (this part is simplified and should be replaced with proper KML parsing)
        # Assuming we parse the data into a list of dictionaries with the keys: date_utc, parameter, value, latitude, longitude, unit
        parsed_data = parse_kml_data(kml_data)

        # Store the data in the database
        store_data_in_db(parsed_data)

    except Exception as e:
        print(f"Unable to perform AirNowAPI request. {e}")
        sys.exit(1)

def parse_kml_data(kml_data):
    # This function should parse the KML data and return a list of dictionaries with the required fields
    # For the sake of this example, let's return a mock list
    return [
        {
            "date_utc": datetime.strptime("2014-09-23T22:00:00", "%Y-%m-%dT%H:%M:%S"),
            "parameter": "o3",
            "value": 0.03,
            "latitude": 34.0522,
            "longitude": -118.2437,
            "unit": "ppm"
        },
        {
            "date_utc": datetime.strptime("2014-09-23T23:00:00", "%Y-%m-%dT%H:%M:%S"),
            "parameter": "pm25",
            "value": 12.0,
            "latitude": 34.0522,
            "longitude": -118.2437,
            "unit": "µg/m³"
        }
    ]

def store_data_in_db(parsed_data):
    # Database connection
    DATABASE_URL = "postgresql+psycopg2://username:password@localhost/dbname"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Create the table if it doesn't exist
    Base.metadata.create_all(engine)

    # Add parsed data to the session and commit to the database
    for data in parsed_data:
        reading = USAirQualityReading(
            date_utc=data["date_utc"],
            parameter=data["parameter"],
            value=data["value"],
            latitude=data["latitude"],
            longitude=data["longitude"],
            unit=data["unit"]
        )
        session.add(reading)
    session.commit()
    session.close()

if __name__ == "__main__":
    main()