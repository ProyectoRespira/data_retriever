from src.models import StationReadings, CalibrationFactors, StationsReadingsRaw, Stations
from datetime import timedelta
from sqlalchemy import func, cast, Float, and_, Time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import logging
from datetime import timedelta
from sqlalchemy import func, and_, cast, Float, Time

# Function to get the earliest and latest dates
def get_date_range(postgres_session):
    earliest_date = postgres_session.query(func.min(StationReadings.date)).scalar()
    latest_date = postgres_session.query(func.max(StationReadings.date)).scalar()
    return earliest_date, latest_date

# Function to get all stations
def get_stations(postgres_session):
    return postgres_session.query(Stations).all()

# Function to calculate the average PM2.5 value for a given date range
def calculate_pattern_avg(postgres_session, start_date, end_date):
    return postgres_session.query(func.avg(StationReadings.pm2_5)).filter(
        StationReadings.date >= start_date,
        StationReadings.date <= end_date,
        StationReadings.pm2_5.isnot(None)
    ).scalar()

# Function to check if a calibration factor already exists
def check_existing_factor(postgres_session, station_id, start_date, end_date):
    return postgres_session.query(CalibrationFactors).filter(
        CalibrationFactors.station_id == station_id,
        CalibrationFactors.start_date == start_date,
        CalibrationFactors.end_date == end_date
    ).first()

# Function to calculate the station average PM2.5 value for a given date range
def calculate_station_avg(postgres_session, station_id, start_date, end_date):
    return postgres_session.query(func.avg(cast(StationsReadingsRaw.mp2_5, Float))).filter(
        StationsReadingsRaw.station_id == station_id,
        and_(
            func.to_date(StationsReadingsRaw.fecha, 'DD-MM-YYYY') + func.cast(StationsReadingsRaw.hora, Time) >= start_date,
            func.to_date(StationsReadingsRaw.fecha, 'DD-MM-YYYY') + func.cast(StationsReadingsRaw.hora, Time) <= end_date,
            StationsReadingsRaw.mp2_5.isnot(None)
        )
    ).scalar()

# Main function to calculate calibration factors
def calculate_calibration_factor(postgres_session):
    logging.info('Starting calculate_calibration_factor...')
    factors = []

    try:
        # Get the date range and stations
        earliest_airnow_date, latest_airnow_date = get_date_range(postgres_session)
        stations = get_stations(postgres_session)

        if not earliest_airnow_date:
            logging.info('No sufficient data in airnow_data table.')
            return factors

        current_start_date = earliest_airnow_date

        # Loop through 90-day intervals until the latest date
        while current_start_date + timedelta(days=90) <= latest_airnow_date:
            current_end_date = current_start_date + timedelta(days=90) 

            # Calculate the pattern average for the current date range
            pattern_avg = calculate_pattern_avg(postgres_session, current_start_date, current_end_date)

            # Loop through each station to calculate calibration factors
            for station in stations:
                # Check if a calibration factor already exists for the current period and station
                if check_existing_factor(postgres_session, station.id, current_start_date, current_end_date):
                    continue

                logging.info(f'Calculating factor for period: {current_start_date} / {current_end_date} for station {station.id}')

                # Calculate the station average for the current date range
                station_avg = calculate_station_avg(postgres_session, station.id, current_start_date, current_end_date)

                # Calculate the calibration factor if both averages are not None
                if pattern_avg is not None and station_avg is not None:
                    station_calibration_factor = pattern_avg / station_avg
                    factors.append({
                        'station_id': station.id,
                        'start_date': current_start_date,
                        'end_date': current_end_date,
                        'station_mean': station_avg,
                        'pattern_mean': pattern_avg,
                        'calibration_factor': station_calibration_factor
                    })

            # Move to the next 90-day period
            current_start_date = current_end_date

    except Exception as e:
        # Log any errors that occur
        logging.error(f'An error occurred: {e}', exc_info=True)
    
    return factors

def prepare_calibration_factors_for_insertion(postgres_session):
    factors = calculate_calibration_factor(postgres_session)
    calibration_factors = []
    for data in factors:
        calibration_factors.append(CalibrationFactors(
            start_date=data['start_date'],
            end_date=data['end_date'],
            station_id=data['station_id'],
            station_mean=data['station_mean'],
            pattern_mean=data['pattern_mean'],
            calibration_factor=data['calibration_factor']
        ))
    return calibration_factors