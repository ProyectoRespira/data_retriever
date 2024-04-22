from src.models import BasePostgres, Stations
from sqlalchemy.exc import IntegrityError
from src.database import create_postgres_session
import json

with open('src/stations_data.json', 'r') as f:
    stations_data = json.load(f)


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
                    region=station_info['region']
                )
                postgres_session.add(new_station)
                postgres_session.commit()
                print(f"Station '{new_station.name}' created successfully.")
            except KeyError as e:
                print(f"Skipping station creation due to missing or invalid data: {e}")
            except IntegrityError:
                postgres_session.rollback()
                print(f"Failed to create station with ID '{station_id}'. It may already exist.")
        else:
            print(f"Station with ID '{station_id}' already exists. Skipping creation.")


def create_postgres_tables(postgres_engine):
    BasePostgres.metadata.create_all(postgres_engine)
    with create_postgres_session(postgres_engine) as session:
        create_stations(postgres_session=session)