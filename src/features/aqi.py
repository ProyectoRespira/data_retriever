from src.models import StationReadings
from datetime import timedelta
from sqlalchemy import func

# AQI functions

def calculate_aqi_2_5(x):
    if x <= 12:
        return round(x * 50 / 12, 0)
    elif x <= 35.4:
        return round(51 + (x - 12.1) * 49 / 23.3, 0)
    elif x <= 55.4:
        return round(101 + (x - 35.5) * 49 / 19.9, 0)
    elif x <= 150.4:
        return round(151 + (x - 55.5) * 49 / 94.4, 0)
    elif x <= 250.4:
        return round(201 + (x - 150.5) * 99 / 99.9, 0)
    elif x <= 350.4:
        return round(301 + (x - 250.5) * 99 / 99.9, 0)
    else:
        return round(401 + (x - 350.5) * 99 / 149.9, 0)

def calculate_aqi_10(x):
    if x <= 54:
        return round(x * 50 / 54, 0)
    elif x <= 154:
        return round(51 + (x - 55) * 49 / 99, 0)
    elif x <= 254:
        return round(101 + (x - 155) * 49 / 99, 0)
    elif x <= 354:
        return round(151 + (x - 255) * 49 / 99, 0)
    elif x <= 424:
        return round(201 + (x - 355) * 99 / 69, 0)
    elif x <= 504:
        return round(301 + (x - 425) * 99 / 79, 0)
    else:
        return round(401 + (x - 504) * 99 / 100, 0)

def query_station_readings(session, station_id):
    '''
    Fetch readings for a specific station where AQI values need to be updated.
    '''
    return session.query(StationReadings).filter(
        StationReadings.station == station_id,
        (StationReadings.aqi_pm2_5 == None) | (StationReadings.aqi_pm10 == None)
    ).all()

def calculate_24h_mean(session, station_id, reading_date):
    '''
    Calculate the 24-hour mean for PM2.5 and PM10 values for a specific station.
    '''
    pm2_5_24h_mean = session.query(func.avg(StationReadings.pm2_5)).filter(
        StationReadings.station == station_id,
        StationReadings.date >= reading_date - timedelta(hours=24),
        StationReadings.date <= reading_date
    ).scalar()

    pm10_24h_mean = session.query(func.avg(StationReadings.pm10)).filter(
        StationReadings.station == station_id,
        StationReadings.date >= reading_date - timedelta(hours=24),
        StationReadings.date <= reading_date
    ).scalar()

    return pm2_5_24h_mean, pm10_24h_mean

def compute_aqi(pm2_5_mean, pm10_mean):
    '''
    Compute AQI values for given PM2.5 and PM10 means.
    '''
    aqi_pm2_5 = calculate_aqi_2_5(pm2_5_mean) if pm2_5_mean is not None else None
    aqi_pm10 = calculate_aqi_10(pm10_mean) if pm10_mean is not None else None
    return aqi_pm2_5, aqi_pm10

def update_station_reading_with_aqi(reading, aqi_pm2_5, aqi_pm10):
    '''
    Update a station reading with computed AQI values.
    '''
    if aqi_pm2_5 is not None:
        reading.aqi_pm2_5 = aqi_pm2_5
    if aqi_pm10 is not None:
        reading.aqi_pm10 = aqi_pm10

def compute_and_update_aqi_for_station_readings(session, station_id):
    '''
    Calculate AQI 2.5 and AQI 10 for existing pm readings in StationReadings
    and update table with AQI values.
    '''
    readings = query_station_readings(session, station_id)

    for reading in readings:
        pm2_5_24h_mean, pm10_24h_mean = calculate_24h_mean(session, station_id, reading.date)
        aqi_pm2_5, aqi_pm10 = compute_aqi(pm2_5_24h_mean, pm10_24h_mean)
        update_station_reading_with_aqi(reading, aqi_pm2_5, aqi_pm10)

    session.commit()