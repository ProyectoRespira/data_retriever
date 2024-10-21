from src.models import StationsReadingsRaw, WeatherStations, WeatherReadings, StationReadings, Stations, Regions
from sqlalchemy import func, desc
from datetime import datetime

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def query_last_raw_measurement_id(postgres_session, station_id):
    """
    Get the last measurement ID for a specific station in StationReadingsRaw.
    
    Parameters:
    - postgres_session: The PostgreSQL session object.
    - station_id: The ID of the weather station.

    Returns:
    - The ID of the last measurement for the specified station, or 0 if no measurements exist.
    """
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

def query_last_stationreadings_timestamp(session, station_id):
    last_measurement = session.query(
                        func.max(StationReadings.date)
                         ).filter(StationReadings.station==station_id).scalar()
    if last_measurement:
        logging.info(f'Last transformation timestamp: {last_measurement}')
        return last_measurement
    else:
        logging.info(f'no previous measurements for station {station_id}')
        return datetime(2019, 1, 1, 0, 0, 0)

def fetch_weather_stations_ids(session):
    """
    Get the IDs of all weather stations.

    Parameters:
    - session: The database session object.

    Returns:
    - A list of IDs for all weather stations.
    """
    result = session.query(WeatherStations.id).all()
    station_ids = [r[0] for r in result]
    return station_ids

def fetch_weather_station_coordinates(session, station_id): 
    """
    Get the latitude and longitude for a specific weather station.

    Parameters:
    - session: The database session object.
    - station_id: The ID of the weather station.

    Returns:
    - A tuple containing the latitude and longitude of the specified weather station.
    """
    latitude = session.query(WeatherStations.latitude).filter(
        WeatherStations.id == station_id
    ).scalar()
    longitude = session.query(WeatherStations.longitude).filter(
        WeatherStations.id == station_id
    ).scalar()
    return latitude, longitude

def fetch_last_weather_station_timestamp(session, station_id):
    """
    Get the last timestamp for a specific weather station from WeatherReadings.

    Parameters:
    - session: The database session object.
    - station_id: The ID of the weather station.

    Returns:
    - The most recent timestamp for the specified weather station.
    """
    return session.query(func.max(WeatherReadings.date)).filter(
        WeatherReadings.weather_station == station_id
    ).scalar()

def fetch_last_station_readings_timestamp(session, station_id):
    """
    Get the last StationReadings timestamp for a specific station.

    Parameters:
    - session: The database session object.
    - station_id: The ID of the station.

    Returns:
    - The most recent timestamp from StationReadings for the specified station.
    """
    last_timestamp = session.query(
        func.max(StationReadings.date)
        ).join(
        Stations, StationReadings.station == Stations.id
        ).filter(
            Stations.id == station_id
        ).scalar()

    return last_timestamp

def fetch_pattern_station_ids(session):
    """
    Get the IDs of all pattern stations.

    Parameters:
    - session: The database session object.

    Returns:
    - A list of IDs for all pattern stations.
    """
    pattern_station_ids = session.query(Stations.id).filter(
        Stations.is_pattern_station == True
    ).all()

    return [id_tuple[0] for id_tuple in pattern_station_ids]

def fetch_station_ids(session):
    """
    Get the IDs of all stations that are not pattern stations.

    Parameters:
    - session: The database session object.

    Returns:
    - A list of IDs for all stations that are not pattern stations.
    """
    station_ids = session.query(Stations.id).filter(
        Stations.is_pattern_station == False
    ).all()

    return [id_tuple[0] for id_tuple in station_ids]

def fetch_region_bbox(session, region_code):
    """
    Get the bounding box (bbox) for a specific region.

    Parameters:
    - session: The database session object.
    - region_code: The code of the region.

    Returns:
    - The bounding box of the specified region.
    """
    bbox = session.query(Regions.bbox).filter(
        Regions.region_code == region_code
    ).scalar()

    return bbox

def fetch_station_readings_count(session, station_id):
    """
    Get the count of readings for a specific station.

    Parameters:
    - session: The database session object.
    - station_id: The ID of the station.

    Returns:
    - The count of readings for the specified station.
    """
    count = session.query(
        func.count(StationReadings.id)
    ).filter(
        StationReadings.station == station_id
    ).scalar()

    return count

def fetch_station_region_code(session, station_id):
    """
    Get the region code for a specific station.

    Parameters:
    - session: The database session object.
    - station_id: The ID of the station.

    Returns:
    - The region code of the specified station.
    """
    return session.query(Stations.region).filter(
        Stations.id == station_id
    ).scalar()

def fetch_pattern_station_id_region(session, station_id):
    '''
    Returns pattern_station_id and region for a specific station_id.
    '''
    region = session.query(Stations.region).filter(
        Stations.id == station_id
    ).scalar()

    pattern_station_id = session.query(Stations.id).filter(
        Stations.is_pattern_station == True,
        Stations.region == region
    ).scalar()
    return pattern_station_id, region