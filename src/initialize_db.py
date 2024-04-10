from src.models import BasePostgres, Stations
from sqlalchemy.exc import IntegrityError
from src.database import create_postgres_session

stations_data = [
    {
        "id": 1,
        "name": "Campus de la UNA",
        "latitude": "-25.33360102213910",
        "longitude": "-57.5139365997165", 
        "region": "CENTRAL"
    },
    {
        "id": 2,
        "name": "Zona Multiplaza",
        "latitude": "-25.32014521770180",
        "longitude": "-57.56050041876730", 
        "region": "ASUNCION"
    },
    {
        "id": 3,
        "name": "Acceso Sur",
        "latitude": "-25.34024024382230",
        "longitude": "-57.58431466296320",
        "region": "CENTRAL"
    },
    {
        "id": 4,
        "name": "Primero de Marzo y Perón",
        "latitude": "-25.32836979255080",
        "longitude": "-57.62706899084150",
        "region": "ASUNCION"
    },
    {
        "id": 5,
        "name": "Villa Morra",
        "latitude": "-25.29511316679420",
        "longitude": "-57.57708610966800",
        "region": "ASUNCION"
    },
    {
        "id": 6,
        "name": "Barrio Jara",
        "latitude": "-25.28833455406130",
        "longitude": "-57.60329900309440",
        "region": "ASUNCION"
    },
    {
        "id": 7,
        "name": "San Roque",
        "latitude": "-25.28936695307490",
        "longitude": "-57.62515967711810",
        "region": "ASUNCION"
    },
    {
        "id": 8,
        "name": "Centro de Asunción",
        "latitude": "-25.28640403412280",
        "longitude": "-57.64701121486720",
        "region": "ASUNCION"
    },
    {
        "id": 9,
        "name": "Ñu Guasu",
        "latitude": "-25.26458493433890",
        "longitude": "-57.54793468862770",
        "region": "ASUNCION"
    },
    {
        "id": 10,
        "name": "Botánico",
        "latitude": "-25.24647398851810",
        "longitude": "-57.54928501322870",
        "region": "ASUNCION"
    }
]

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


        
    