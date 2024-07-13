from src.models import StationReadings, CalibrationFactors, StationsReadingsRaw, Stations, WeatherReadings, WeatherStations, Regions
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import func, and_, Time, distinct
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Helper Functions

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

def query_raw_pm_readings(session, station_id, start, end):
    '''
    Queries raw PM2.5 readings for a specific station and time range.
    '''
    pm_readings = session.query(
        StationsReadingsRaw.fecha,
        StationsReadingsRaw.hora,
        StationsReadingsRaw.mp2_5
    ).filter(
        StationsReadingsRaw.station_id == station_id,
        and_(
            func.to_date(StationsReadingsRaw.fecha, 'DD-MM-YYYY') + func.cast(StationsReadingsRaw.hora, Time) >= start,
            func.to_date(StationsReadingsRaw.fecha, 'DD-MM-YYYY') + func.cast(StationsReadingsRaw.hora, Time) <= end,
            StationsReadingsRaw.mp2_5.isnot(None)
        )
    ).all()

    return pm_readings

def query_humidity_readings(session, station_id, start, end):
    '''
    Queries humidity readings for a specific station and time range.
    '''
    _, region = fetch_pattern_station_id_region(session, station_id)

    humidity_readings = session.query(
        WeatherReadings.date,
        WeatherReadings.humidity
    ).join(
        WeatherStations, WeatherReadings.weather_station == WeatherStations.id
    ).join(
        Regions, WeatherStations.region == Regions.region_code
    ).filter(
        Regions.region_code == region,
        WeatherReadings.date >= start,
        WeatherReadings.date <= end
    ).all()

    return humidity_readings

def query_pattern_station_readings(session, pattern_station_id, start, end):
    '''
    Queries PM2.5 readings for the pattern station within a specified time range.
    '''
    logging.info(f'Querying pattern station readings for ID: {pattern_station_id}, Start: {start}, End: {end}')
    
    pattern_readings = session.query(
        StationReadings.pm2_5,
        StationReadings.date
        ).filter(
        StationReadings.station == pattern_station_id,
        StationReadings.date >= start,
        StationReadings.date <= end
    ).all()

    return pattern_readings

def get_unique_calibration_dates(session, pattern_station_id):
    dates = session.query(
        func.distinct(func.date_trunc('month', StationReadings.date))
        ).filter(
            StationReadings.station == pattern_station_id
        ).order_by(
            func.date_trunc('month', StationReadings.date)
        ).all()
    
    unique_dates = [date[0] for date in dates]

    return unique_dates

# Data Processing Functions

def create_raw_pm25_dataframe(pm_readings):
    '''
    Creates a DataFrame for PM2.5 readings.
    '''
    pm_df = pd.DataFrame(pm_readings, columns=['fecha', 'hora', 'mp2_5'])
    pm_df['datetime'] = pd.to_datetime(pm_df['fecha'] + ' ' + pm_df['hora'].astype(str), format='%d-%m-%Y %H:%M')
    pm_df = pm_df.set_index('datetime').drop(columns=['fecha', 'hora'])
    pm_df['mp2_5'] = pd.to_numeric(pm_df['mp2_5'], errors='coerce')

    if pm_df.index.duplicated().any():
        pm_df = pm_df[~pm_df.index.duplicated(keep='first')]

    return pm_df

def create_crh_dataframe(humidity_readings):
    '''
    Creates a DataFrame for humidity readings and calculates C_RH.
    '''
    #Note: C_RH is the humidity correction factor for pm given regional humidity levels in the air

    # About the lambda function - humidity correction: https://amt.copernicus.org/articles/11/709/2018/
    # - for humidity < 65 = 1
    # - for 65 < humidity < 85 = humidity * 0.0121212
    # - for humidity > 85 = equation 6 in paper
    
    # Note: re-check regional k is 0.2 for Paraguay -> https://sci-hub.se/https://doi.org/10.5194/acp-10-5241-2010
    
    humidity_df = pd.DataFrame(humidity_readings, columns=['date', 'humidity'])
    humidity_df['C_RH'] = humidity_df['humidity'].apply(lambda x: 1 if x < 65 else (0.0121212 * x) + 1 if x < 85 else (1 + ((0.2 / 1.65) / (-1 + 100 / min(x, 90)))))
    humidity_df = humidity_df.set_index('date').drop(columns=['humidity'])

    if humidity_df.index.duplicated().any():
        humidity_df = humidity_df[~humidity_df.index.duplicated(keep='first')]
    return humidity_df

def compute_corrected_pm_average(pm_df, humidity_df):
    '''
    Computes the corrected average PM2.5 readings.
    '''
    merged_df = pm_df.join(humidity_df.resample('5min').ffill(), how='left')
    merged_df['pm2_5_corrected'] = merged_df['mp2_5'] / merged_df['C_RH']
    corrected_average_pm2_5 = float(merged_df['pm2_5_corrected'].mean())
    return corrected_average_pm2_5

# Main Workflow Functions

def get_station_avg_corrected_for_humidity(session, station_id, start, end):
    """
    Calculates the average PM2.5 readings corrected for humidity for a specific station over a given time period.
    """
    pm_readings = query_raw_pm_readings(session, station_id, start, end)
    humidity_readings = query_humidity_readings(session, station_id, start, end)

    pm_df = create_raw_pm25_dataframe(pm_readings)
    humidity_df = create_crh_dataframe(humidity_readings)

    corrected_average_pm2_5 = compute_corrected_pm_average(pm_df, humidity_df)

    return corrected_average_pm2_5

def compute_pattern_station_average_pm25(session, pattern_station_id, start, end):
    '''
    Computes the average PM2.5 readings for the pattern station within a specified time range.
    '''
    pattern_readings = query_pattern_station_readings(session, pattern_station_id, start, end)
    if pattern_readings:
        pattern_average = float(sum([reading.pm2_5 for reading in pattern_readings]) / len(pattern_readings))
    else:
        logging.warning(f'No readings found.')
        pattern_average = None
    return pattern_average

def setup_calibration_date_ranges(month_year):
    '''
    Sets up calibration and usage date ranges based on a given calibration date.
    '''
    start_cal = month_year - relativedelta(months = 3)
    end_cal = month_year
    start_usage = month_year + timedelta(hours=1)
    end_usage = start_usage + relativedelta(months = 1)

    return start_cal, end_cal, start_usage, end_usage

def verify_data_availability_for_calibration(session, month_year, station_id):
    '''
    Verify that there's sufficient data available for calibration by checking there's data
    from both stations in start_cal date.
    '''
    pattern_station_id, _ = fetch_pattern_station_id_region(session, station_id)
    start_cal, end_cal, _, _ = setup_calibration_date_ranges(month_year)

    pattern_data = query_pattern_station_readings(session, pattern_station_id, start_cal, end_cal)
    station_data = query_raw_pm_readings(session, station_id, start_cal, end_cal)

    if not pattern_data or not station_data:
        logging.warning('Pattern data or station data is missing')
        return False
    
    r = len(station_data) / len(pattern_data)
    if r < 0.6:
        logging.warning(f'Station {station_id} has less than 60% of valid data during calibration period.')
        return False

    pattern_dates = list(set([reading.date.date() for reading in pattern_data]))
    pattern_dates.sort()
    lowest_pattern_date = pd.Timestamp(pattern_dates[0])

    station_dates = list(set([reading.fecha for reading in station_data]))
    station_dates = pd.to_datetime(station_dates, format='%d-%m-%Y').sort_values()

    if lowest_pattern_date in station_dates:
        logging.info('Lowest pattern date is present in station data')
        return True
    else:
        logging.warning('Lowest pattern date is not present in station data')
        return False
    

def compute_calibration_factor(session, month_year, station_id):
    """
    Calculates the calibration factor for a given station and period.

    Args:
    - session (SQLAlchemy session): The session to use for database queries.
    - month_year (str): The month and year for which calibration is to be calculated.
    - station_id (int): The ID of the station for which calibration is to be calculated.

    Returns:
    - dict or None: A dictionary containing calibration information if successful, 
      None if an error occurs or insufficient data is available.

    The function performs the following steps:
    1. Retrieves pattern station ID and region.
    2. Checks if pattern station ID is valid; logs errors if not and returns None.
    3. Checks if sufficient data exists for calibration; logs information if not and returns None.
    4. Retrieves: 
        - calibration dates, 
        - station PM2.5 mean corrected for humidity, and 
        - pattern station PM2.5 mean.
    5. Calculates calibration factor as pattern mean divided by station mean.
    6. Constructs and returns a dictionary with calibration information.
    """
    logging.info(f'Getting region and pattern station id for station {station_id}')
    
    # Verification

    pattern_station_id, region = fetch_pattern_station_id_region(session, station_id)
    if pattern_station_id is None:
        logging.exception(f'No pattern station for station {station_id}')
        return None

    data_exists = verify_data_availability_for_calibration(session, month_year, station_id)
    if data_exists is False:
        logging.info(f'Not enough data for calibration for station {station_id} for period {month_year} ')
        return None

    # Calibration
    start_cal, end_cal, start_usage, end_usage = setup_calibration_date_ranges(month_year)
    
    station_mean = get_station_avg_corrected_for_humidity(session, station_id, start_cal, end_cal)
    pattern_mean = compute_pattern_station_average_pm25(session, pattern_station_id, start_cal, end_cal)
    calibration_factor = pattern_mean/station_mean

    calibration_info = {'region': region,
                        'station_id': station_id,
                        'date_start_cal': start_cal,
                        'date_end_cal': end_cal,
                        'station_mean': station_mean, 
                        'pattern_mean': pattern_mean,
                        'date_start': start_usage,
                        'date_end': end_usage,
                        'calibration_factor': calibration_factor}

    return calibration_info


def insert_calibration_factor_into_db(session, calibration_info):
    '''
    Inserts the calculated calibration factor into the database.
    '''
    calibration = CalibrationFactors(
        region = calibration_info['region'],
        station_id = calibration_info['station_id'],
        date_start_cal = calibration_info['date_start_cal'],
        date_end_cal = calibration_info['date_end_cal'],
        station_mean = calibration_info['station_mean'],
        pattern_mean = calibration_info['pattern_mean'],
        date_start = calibration_info['date_start'],
        date_end = calibration_info['date_end'],
        calibration_factor = calibration_info['calibration_factor']
    )

    session.add(calibration)
    session.commit()

    logging.info(f"Calibration factor for station {calibration_info['station_id']} "
             f"for period {calibration_info['date_start']}/{calibration_info['date_end']} inserted correctly")
    return True