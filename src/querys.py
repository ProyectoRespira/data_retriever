from src.models import StationsReadingsRaw, WeatherStations, WeatherReadings, StationReadings, Stations, Regions
from sqlalchemy import func, desc

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_last_raw_measurement_id(postgres_session, station_id):
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
    
def get_weather_stations_ids(session):
    result = session.query(WeatherStations.id).all()
    station_ids = [r[0] for r in result]
    return station_ids

def get_weather_station_coordinates(session, station_id): 
    latitude = session.query(WeatherStations.latitude).filter(
        WeatherStations.id == station_id
    ).scalar()
    longitude = session.query(WeatherStations.longitude).filter(
        WeatherStations.id == station_id
    ).scalar()
    return latitude, longitude

def get_last_weather_station_timestamp(session):
    return session.query(func.max(WeatherReadings.date)).scalar()

def get_last_station_readings_timestamp(session, station_id):
    last_timestamp = session.query(
        func.max(StationReadings.date)
        ).join(
        Stations, StationReadings.station == Stations.id
        ).filter(
            Stations.id == station_id
        ).scalar()

    return last_timestamp

def get_pattern_station_ids(session):
    pattern_station_ids = session.query(Stations.id).filter(
        Stations.is_pattern_station == True
    ).all()

    return [id_tuple[0] for id_tuple in pattern_station_ids]

def get_station_ids(session):
    station_ids = session.query(Stations.id).filter(
        Stations.is_pattern_station == False
    ).all()

    return [id_tuple[0] for id_tuple in station_ids]

def get_region_bbox(session, region_code):
    bbox = session.query(Regions.bbox).filter(
        Regions.region_code == region_code
    ).scalar()

    return bbox

def get_station_readings_count(session, pattern_station_id):
    count = session.query(
        func.count(StationReadings.id)
    ).filter(
        StationReadings.station == pattern_station_id
    ).scalar()

    return count

def get_region_code(session, station_id):
    return session.query(Stations.region).filter(
        Stations.id == station_id
    ).scalar()