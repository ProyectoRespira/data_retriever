from database.models import BasePostgres, Stations, Regions
from sqlalchemy.exc import IntegrityError
from database.database import create_postgres_session, create_postgres
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open('database/stations_data.json', 'r') as f:
    stations_data = json.load(f)

with open('database/region_data.json', 'r') as f:
    region_data = json.load(f)


def create_stations(postgres_session, station_data = stations_data):
    existing_station_ids = {station.id for station in postgres_session.query(Stations).all()}
    for station_info in station_data:
        station_id = station_info.get('id')
        if station_id not in existing_station_ids:
            try:
                new_station = Stations(
                    id=station_id,
                    name=station_info['name'],
                    latitude=station_info['latitude'],
                    longitude=station_info['longitude'],
                    region=station_info['region'],
                    is_station_on = station_info['is_station_on']
                )
                postgres_session.add(new_station)
                postgres_session.commit()
                print(f"Station '{new_station.name}' created successfully.")
            except KeyError as e:
                print(f"Skipping station creation due to missing or invalid data: {e}")
            except IntegrityError as e:
                postgres_session.rollback()
                print(f"Failed to create station with ID '{station_id}'. It may already exist.")
                logging.error(f'error: {e}')
        else:
            print(f"Station with ID '{station_id}' already exists. Skipping creation.")

def create_region(postgres_session, region_data = region_data):
    existing_region_ids = {region.id for region in postgres_session.query(Regions).all()}
    for region_info in region_data:
        region_id = region_info.get('id')
        if region_id not in existing_region_ids:
            try:
                new_region = Regions(
                    id = region_id,
                    name = region_info['name'],
                    latitude = region_info['latitude'],
                    longitude = region_info['longitude'],
                    region_code = region_info['region_code']
                )
                postgres_session.add(new_region)
                postgres_session.commit()
                print(f"Region '{new_region.name}' created successfully.")
            except KeyError as e:
                print(f'Skipping region creation due to missing or invalid data: {e}')
            except IntegrityError:
                postgres_session.rollback()
                print(f"Failed to create station with ID '{region_id}. It may already exist.")
        else:
            print(f"Region with ID '{region_id} already exists. Skipping creation.")

def create_postgres_tables():
    try:
        postgres_engine = create_postgres()
        BasePostgres.metadata.create_all(postgres_engine)
        with create_postgres_session(postgres_engine) as session:
            create_region(postgres_session=session)
            create_stations(postgres_session=session)
    except Exception as e:
        logging.error(f'An error occurred: {e}')
    finally:
        session.close()