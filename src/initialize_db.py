from src.models import BasePostgres, Stations, Regions, WeatherStations
from sqlalchemy.exc import IntegrityError
from src.database import create_postgres_session, create_postgres
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open('src/stations_data.json', 'r') as f:
    stations_data = json.load(f)

with open('src/region_data.json', 'r') as f:
    region_data = json.load(f)

with open('src/weather_data.json', 'r') as f:
    weather_data = json.load(f)

with open('src/pattern_data.json', 'r') as f:
    pattern_data = json.load(f)


def create_stations(postgres_session, station_data = stations_data, pattern_data = pattern_data):
    all_stations = station_data + [station for station in pattern_data]
    existing_station_ids = {station.id for station in postgres_session.query(Stations).all()}
    for station_info in all_stations:
        station_id = station_info.get('id')
        if station_id not in existing_station_ids:
            try:
                new_station = Stations(
                    id=station_id,
                    name=station_info['name'],
                    latitude=station_info['latitude'],
                    longitude=station_info['longitude'],
                    region=station_info['region'],
                    is_station_on = station_info['is_station_on'],
                    is_pattern_station = station_info['is_pattern_station']
                )
                postgres_session.add(new_station)
                postgres_session.commit()
                print(f"Station '{new_station.name}' created successfully.")
            except KeyError as e:
                print(f"Skipping station '{station_id}'creation due to missing or invalid data: {e}")
            except IntegrityError as e:
                postgres_session.rollback()
                print(f"Failed to create station '{station_id}'. It may already exist.")
                logging.error(f'error: {e}')
        else:
            print(f"Station '{station_id}' already exists. Skipping creation.")

def create_region(postgres_session, region_data = region_data):
    existing_region_ids = {region.id for region in postgres_session.query(Regions).all()}
    for region_info in region_data:
        region_id = region_info.get('id')
        if region_id not in existing_region_ids:
            try:
                new_region = Regions(
                    id = region_id,
                    name = region_info['name'],
                    region_code = region_info['region_code'],
                    bbox = region_info['bbox'],
                    has_weather_data = region_info['has_weather_data'],
                    has_pattern_station = region_info['has_pattern_station']
                )
                postgres_session.add(new_region)
                postgres_session.commit()
                print(f"Region '{new_region.name}' created successfully.")
            except KeyError as e:
                print(f"Skipping region with ID = '{region_id}' creation due to missing or invalid data: {e}")
            except IntegrityError:
                postgres_session.rollback()
                print(f"Failed to create region with ID '{region_id}'. It may already exist.")
        else:
            print(f"Region with ID '{region_id}' already exists. Skipping creation.")

def create_weather_stations(postgres_session, weather_data = weather_data):
    existing_weather_ids = {weather.id for weather in postgres_session.query(WeatherStations).all()}
    for weather_info in weather_data:
        weather_id = weather_info.get('id')
        if weather_id not in existing_weather_ids:
            try:
                new_weather_station = WeatherStations(
                    id = weather_id,
                    name = weather_info['name'],
                    latitude = weather_info['latitude'],
                    longitude = weather_info['longitude'],
                    region = weather_info['region']
                )
                postgres_session.add(new_weather_station)
                postgres_session.commit()
                print(f"Weather Station '{new_weather_station.name}' created successfully.")
            except KeyError as e:
                print(f"Skipping Weather Station with ID = '{weather_id}' creation due to missing or invalid data: {e}")
            except IntegrityError:
                postgres_session.rollback()
                print(f"Failed to create Weather Station with ID = '{weather_id}'. It may already exist.")
        else:
            print(f"Weather Station with ID '{weather_id}' already exists. Skipping creation.")



def create_postgres_tables():
    try:
        postgres_engine = create_postgres()
        BasePostgres.metadata.create_all(postgres_engine)
        with create_postgres_session(postgres_engine) as session:
            create_region(postgres_session=session)
            create_weather_stations(postgres_session=session)
            create_stations(postgres_session=session)
    except Exception as e:
        logging.error(f'An error occurred in create_postgres_tables: {e}')
    finally:
        session.close()