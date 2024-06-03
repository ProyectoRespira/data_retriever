from sqlalchemy import MetaData, Table, desc, select
from sqlalchemy.exc import SQLAlchemyError
from src.models import StationsReadingsRaw, WeatherData, StationReadings, USAirQualityReadings
from src.time_utils import convert_to_utc
from datetime import datetime, timedelta
from pytz import timezone
from meteostat import Point, Hourly
from sqlalchemy import func
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# mirror.py

def get_last_measurement_id(postgres_session, station_id):
    logging.info('Starting get_last_measurement_id...')
    last_measurement = (postgres_session.query(StationsReadingsRaw)
                        .filter(StationsReadingsRaw.station_id == station_id)
                        .order_by(desc(StationsReadingsRaw.measurement_id))
                        .first())
    if last_measurement:
        logging.info(f'Last measurement ID for station {station_id}: {last_measurement.measurement_id}')
        return last_measurement.measurement_id
    else:
        logging.info(f'No previous measurements for station {station_id}')
        return 0

def select_new_records_from_origin_table(mysql_engine, table_name, last_measurement_id):
    logging.info(f'Starting select_new_records_from_origin_table where table_name = {table_name} and last_measurement_id = {last_measurement_id}')

    try:
        metadata = MetaData()
        metadata.reflect(bind=mysql_engine)
        table = Table(table_name, metadata, autoload_with=mysql_engine)
        
        column_names = [column.name for column in table.columns]
        column_expressions = [column for column in table.columns]
        query = select(*column_expressions).where(table.c.ID > last_measurement_id)

        with mysql_engine.connect() as connection:
            result = connection.execute(query)
            records_as_dicts = [dict(zip(column_names, row)) for row in result.fetchall()]
        records_as_dicts_lower = [{key.lower(): value for key, value in record.items()} for record in records_as_dicts]

        logging.info(f'Selected {len(records_as_dicts_lower)} new records from table {table_name}')
        #print(records_as_dicts_lower)
        return records_as_dicts_lower
    except SQLAlchemyError as e:
        logging.error(f"Error occurred: {e}")
        return None

# meteostat_data.py

def fetch_meteostat_data(start, end):
    logging.info('fetching meteostat data...')
    asuncion = Point(-25.2667, -57.6333, 101)
    data = Hourly(asuncion, start, end).fetch()
    return data


def get_last_meteostat_timestamp(session):
    return session.query(func.max(WeatherData.date)).scalar()

def determine_time_range(session):
    if session.query(WeatherData).count() == 0:
        start_utc = datetime(2019, 1, 1, 0, 0, 0, 0)
    else:
        last_meteostat_timestamp = get_last_meteostat_timestamp(session)
        start_utc = convert_to_utc(last_meteostat_timestamp + timedelta(hours=1))
    
    end_utc = datetime.now(timezone('UTC')).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    
    return start_utc, end_utc


# airnow data
def get_last_airnow_timestamp(session):
    return session.query(func.max(USAirQualityReadings.date)).scalar()

def define_airnow_api_url(session):
    try:
        load_dotenv()
    except:
        raise "Error loading .env file right now"

    if session.query(USAirQualityReadings).count() == 0:
        last_airnow_timestamp_utc = datetime(2023, 1, 1, 0, 0, 0, 0)
    else:
        last_airnow_timestamp_localtime = get_last_airnow_timestamp(session)
        last_airnow_timestamp_utc = convert_to_utc(last_airnow_timestamp_localtime)
    
    options = {}
    options["url"] = "https://airnowapi.org/aq/data/"
    options["start_date"] = last_airnow_timestamp_utc.strftime('%Y-%m-%d')
    options["start_hour_utc"] = last_airnow_timestamp_utc.strftime('%H')
    options["end_date"] = datetime.now(timezone('UTC')).strftime('%Y-%m-%d')
    options["end_hour_utc"] = datetime.now(timezone('UTC')).strftime('%H')
    options["parameters"] = "pm25"
    options["bbox"] = "-57.725,-25.384,-57.500,-25.214"
    options["data_type"] = "c" # options: a (AQI), b (concentrations & AQI), c (concentrations)
    options["format"] = "application/json" # options: 'text/csv', 'application/json', 'application/vnd.google-earth.kml', 'application/xml'
    options["api_key"] = os.getenv('AIRNOW_API_KEY')
    options["verbose"] = 1
    options["includerawconcentrations"] = 1

    # API request URL
    request_url = options["url"] \
                  + "?startdate=" + options["start_date"] \
                  + "t" + options["start_hour_utc"] \
                  + "&enddate=" + options["end_date"] \
                  + "t" + options["end_hour_utc"] \
                  + "&parameters=" + options["parameters"] \
                  + "&bbox=" + options["bbox"] \
                  + "&datatype=" + options["data_type"] \
                  + "&format=" + options["format"] \
                  + "&api_key=" + options["api_key"]
    
    return request_url

