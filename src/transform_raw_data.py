from sqlalchemy import func, distinct, and_
from datetime import datetime, timedelta
from src.models import StationsReadingsRaw, StationReadings, Stations
from src.database import create_postgres, create_postgres_session
import pandas as pd

def apply_correction_factor(station_id, pm, date):
    # Example function to get correction factor based on station_id
    
    # NOTE_1: Missing humidity correction (humidity data from meteostat) - Now it'll be noisy
    
    # NOTE_2: Factors should be updated every 3 months using data from US Embassy
    
    # NOTE_3: Factors should be different for pm2.5 and pm10, See this paper: 
    # Spatio-Temporal Variations of the PM2.5/PM10 Ratios and 
    # Its Application to Air Pollution Type Classification in China
    # https://www.frontiersin.org/articles/10.3389/fenvs.2021.692440/full
    
    if station_id == 1:
        correction_factor = 3.9477741393724357
    elif station_id == 2:
        correction_factor = 5.773891499911656
    elif station_id == 3:
        correction_factor = 2.2191051063687777
    elif station_id == 4:
        correction_factor = 3.8382500135208697
    elif station_id == 5:
        correction_factor = 39.44025115478353
    elif station_id == 7:
        correction_factor = 9.414312741845814
    elif station_id == 10:
        correction_factor = 8.133051717401786
    else:
        correction_factor = 1.0  # Default correction factor if station_id not found
    
    # Check if the date is on or after 01-01-2024
    if date >= datetime(2024, 1, 1):
        return pm * correction_factor
    else:
        return pm
    
def get_last_transformation_timestamp(session, station_id):
    # Implement logic to retrieve the timestamp of the last transformation from your database
    # Example:
    last_transformation_timestamp = session.query(func.max(StationReadings.date)).filter_by(station = station_id).scalar()
    # Adjust this query according to your database schema
    return last_transformation_timestamp


# def transform_raw_data(session):
#     # Get distinct station IDs
#     station_ids = session.query(distinct(StationsReadingsRaw.station_id)).all()

#     for station_id in station_ids:
#         print(f'populating station_readings table with data from Station {station_id[0]}')
#         # Check if StationReadings table has any data for the current station
#         if session.query(StationReadings).filter_by(station=station_id[0]).count() == 0:
#             # If there is no data for the current station, grab all raw readings for that station
#             print('no previous data for this station...')
#             raw_readings = session.query(StationsReadingsRaw).filter(
#                 StationsReadingsRaw.station_id ==station_id[0]).all()
#             print('getting all readings from current station...')
#         else:
#             # If there is data for the current station, get the timestamp of the last transformation
#             print('getting last transformation timestamp...')
#             last_transformation_timestamp = get_last_transformation_timestamp(session=session, station_id=station_id[0])
#             print('last transformation timestamp retrieved!')
#             # Query only the new raw readings for the current station
#             raw_readings = session.query(StationsReadingsRaw).filter(
#                 StationsReadingsRaw.fecha + ' ' + StationsReadingsRaw.hora > last_transformation_timestamp,
#                 StationsReadingsRaw.station_id == station_id[0]
#             ).all()

#         skipped_records_count = 0

#         last_transformation_time = None 

#         for raw_reading in raw_readings:
#             # Check if fecha and hora are not empty or None
#             if raw_reading.fecha and raw_reading.hora:
#                 if raw_reading.date is None or raw_reading.date.year < 2024:
#                     continue
                
#                 current_hour = raw_reading.date.replace(minute = 0, second = 0, microsecond = 0)
#                 if last_transformation_time is None or current_hour != last_transformation_time:
#                     last_transformation_time = current_hour


#                     start_time = raw_reading.date - timedelta(minutes=raw_reading.date.minute % 60,
#                                                 seconds=raw_reading.date.second,
#                                                 microseconds=raw_reading.date.microsecond)
#                     print(start_time)
                    
#                     end_time = start_time + timedelta(hours = 1)

#                     print(end_time)


#                     hourly_readings = [r for r in raw_readings if (r.station_id == station_id[0]) and start_time <= r.date < end_time]
                    
#                     print(len(hourly_readings))

#                     # Initialize variables to store parsed values
#                     pm1_mean = pm2_5_mean = pm10_mean = temperature_mean = humidity_mean = pressure_mean = None

#                     # Parse and convert each property if it exists
#                     if raw_reading.mp1:
#                         try:
#                             print('pm1_mean...')
#                             pm1_mean = sum(float(reading.mp1) for reading in hourly_readings) / len(hourly_readings)
#                         except:
#                             print(f"Error parsing pm1 for record: {raw_reading.measurement_id}. Setting pm1 to None.")
#                             pm1_mean = None

#                     if raw_reading.mp2_5:
#                         try:
#                             #pm2_5 = float(raw_reading.mp2_5)
#                             pm2_5_mean = sum(float(reading.mp2_5) for reading in hourly_readings) / len(hourly_readings)
#                         except:
#                             print(f"Error parsing pm2_5 for record: {raw_reading.measurement_id}. Setting pm2_5 to None.")
#                             pm2_5_mean = None

#                     if raw_reading.mp10:
#                         try:
#                             #pm10 = float(raw_reading.mp10)
#                             pm10_mean = sum(float(reading.mp10) for reading in hourly_readings) / len(hourly_readings)
#                         except:
#                             print(f"Error parsing pm10 for record: {raw_reading.measurement_id}. Setting pm10 to None.")
#                             pm10_mean = None

#                     if raw_reading.temperatura:
#                         try:
#                             #temperature = float(raw_reading.temperatura)
#                             temperature_mean = sum(float(reading.temperatura) for reading in hourly_readings) / len(hourly_readings)
#                         except:
#                             print(f"Error parsing temperature for record: {raw_reading.measurement_id}. Setting temperature to None.")
#                             temperature_mean = None

#                     if raw_reading.humedad:
#                         try:
#                             #humidity = float(raw_reading.humedad)
#                             humidity_mean = sum(float(reading.humedad) for reading in hourly_readings) / len(hourly_readings)
#                         except:
#                             print(f"Error parsing humidity for record: {raw_reading.measurement_id}. Setting humidity to None.")
#                             humidity_mean = None

#                     if raw_reading.presion:
#                         try:
#                             #pressure = float(raw_reading.presion)
#                             pressure_mean = sum(float(reading.presion) for reading in hourly_readings) / len(hourly_readings)
#                         except:
#                             print(f"Error parsing pressure for record: {raw_reading.measurement_id}. Setting pressure to None.")
#                             pressure_mean = None

                    

#                     # # Apply correction factor for each PM reading if available
#                     # if end_time >= datetime(2024, 1, 1):
#                     #     if pm1_mean is not None:
#                     #         pm1_mean = apply_correction_factor(station_id[0], pm1_mean, end_time)
#                     #     else:
#                     #         corrected_pm1 = None

#                     #     if pm2_5_mean is not None:
#                     #         pm2_5_mean = apply_correction_factor(station_id[0], pm2_5_mean, end_time)
#                     #     else:
#                     #         corrected_pm2_5 = None

#                     #     if pm10_mean is not None:
#                     #         corrected_pm10 = apply_correction_factor(station_id[0], pm10_mean, end_time)
#                     #     else:
#                     #         corrected_pm10 = None

#                     # Query station details
#                     station = session.query(Stations).filter_by(id=raw_reading.station_id).first()

#                     # Create a new StationReadings object with corrected PM readings
#                     reading = StationReadings(
#                         station=station.id,
#                         date=end_time,
#                         pm1=pm1_mean,
#                         pm2_5=pm2_5_mean,
#                         pm10=pm10_mean,
#                         temperature=temperature_mean,
#                         humidity=humidity_mean,
#                         pressure=pressure_mean,
#                         # Set AQI values to None initially
#                         aqi_pm2_5=None,
#                         aqi_pm10=None
#                     )
#                     # Add the new reading to the session
#                     session.add(reading)
#                 else:
#                     skipped_records_count += 1
        
#         print(f'Station {station_id[0]}: Number of records skipped ={skipped_records_count}')
#     # Commit the changes to the database
#         session.commit()



def transform_raw_data(session): # using pandas 
    station_ids = session.query(distinct(StationsReadingsRaw.station_id)).all()

    for station_id in station_ids:
        print(f'Populating station_readings table with data from Station {station_id[0]}')
        
        # Check if StationReadings table has any data for the current station
        if session.query(StationReadings).filter_by(station=station_id[0]).count() == 0:
            # If there is no data for the current station, grab all raw readings for that station
            print('No previous data for this station...')
            raw_readings = session.query(StationsReadingsRaw).filter(
                StationsReadingsRaw.station_id == station_id[0]
            ).all()
        else:
            # If there is data for the current station, get the timestamp of the last transformation
            print('Getting last transformation timestamp...')
            last_transformation_timestamp = get_last_transformation_timestamp(session=session, station_id=station_id[0])
            print('Last transformation timestamp retrieved!')
            print(last_transformation_timestamp)
            # Query only the new raw readings for the current station

            raw_readings_query = session.query(StationsReadingsRaw).filter(
                    and_(
                        StationsReadingsRaw.fecha != '0-0-2000',
                        StationsReadingsRaw.hora != ':0',
                        func.to_timestamp(func.concat(StationsReadingsRaw.fecha, ' ', StationsReadingsRaw.hora), 'DD-MM-YYYY HH24:MI') > last_transformation_timestamp,
                        StationsReadingsRaw.station_id == station_id[0]
                    )
                )
            raw_readings = raw_readings_query.all()
            print([item.date for item in raw_readings])

        if not raw_readings:
            print('No new data for this station.')
            continue

        # Convert raw readings to DataFrame
        df = pd.DataFrame([vars(reading) for reading in raw_readings])

        # Replace invalid datetime values with NaN
        df['fecha_hora'] = df['fecha'] + ' ' + df['hora']
        df['datetime'] = pd.to_datetime(df['fecha_hora'], format='%d-%m-%Y %H:%M', errors='coerce')

        # Drop rows with NaN datetime values
        df = df.dropna()

        # Convert relevant columns to numeric
        numeric_columns = ['mp1', 'mp2_5', 'mp10', 'temperatura', 'humedad', 'presion']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
        df.fillna(value = -144, inplace = True)
        # Group raw readings by hour
        df['hour'] = df['datetime'].dt.floor('h')
        hourly_readings = df.groupby('hour').agg({
            'mp1': 'mean',
            'mp2_5': 'mean',
            'mp10': 'mean',
            'temperatura': 'mean',
            'humedad': 'mean',
            'presion': 'mean'
        }).reset_index()

        # Iterate over hourly readings
        for _, row in hourly_readings.iterrows():
            
            existing_reading = session.query(StationReadings).filter_by(station=station_id[0], date=row['hour']).first()
            
            if existing_reading:
                # Update the existing entry with the new readings
                existing_reading.pm1 = row['mp1']
                existing_reading.pm2_5 = row['mp2_5']
                existing_reading.pm10 = row['mp10']
                existing_reading.temperature = row['temperatura']
                existing_reading.humidity = row['humedad']
                existing_reading.pressure = row['presion']
                existing_reading.aqi_pm2_5 = None
                existing_reading.aqi_pm10 = None
            else:
                # Create a new StationReadings object
                reading = StationReadings(
                    station=station_id[0],
                    date=row['hour'],  # End of the hour
                    pm1=row['mp1'],
                    pm2_5=row['mp2_5'],
                    pm10=row['mp10'],
                    temperature=row['temperatura'],
                    humidity=row['humedad'],
                    pressure=row['presion'],
                    # Set AQI values to None initially
                    aqi_pm2_5=None,
                    aqi_pm10=None
                )
                # Add the new reading to the session
                session.add(reading)

        # Commit the changes to the database
        session.commit()
        print('commited!')


def calculate_aqi(session):
    # Get distinct station IDs
    station_ids = session.query(distinct(StationReadings.station)).all()

    # Iterate over each station ID
    for station_id in station_ids:
        # Get all readings for the current station
        raw_readings = session.query(StationReadings).filter(
            StationReadings.station_id == station_id[0]
        ).all()

        for reading in raw_readings:
            # Calculate 24-hour rolling averages
            pm2_5_24h_mean = session.query(func.avg(StationReadings.pm2_5)).filter(
                StationReadings.station == reading.station_id,
                StationReadings.date >= reading.date - timedelta(hours=24),
                StationReadings.date <= reading.date
            ).scalar()

            pm10_24h_mean = session.query(func.avg(StationReadings.pm10)).filter(
                StationReadings.station_id == reading.station_id,
                StationReadings.date >= reading.date - timedelta(hours=24),
                StationReadings.date <= reading.date
            ).scalar()

            # Calculate AQI for pm2_5
            aqi_pm2_5 = get_aqi_25(pm2_5_24h_mean)

            # Calculate AQI for pm10
            aqi_pm10 = get_aqi_10(pm10_24h_mean)

            # Update the reading with calculated AQI values
            reading.aqi_pm2_5 = aqi_pm2_5
            reading.aqi_pm10 = aqi_pm10

    # Commit the changes to the database
    session.commit()


def get_aqi_25(x):
    if x <= 12:
        return x * 50 / 12 
    elif x <= 35.4 :
        return 51 + (x - 12.1) * 49 / 23.3
    elif x <= 55.4:
        return 101 + (x - 35.5) * 49 / 19.9
    elif x <= 150.4:
        return 151 + (x - 55.5) * 49 / 94.4
    elif x <= 250.4:
        return 201 + (x - 150.5) * 99 / 99.9
    elif x <= 350.4:
        return 301 + (x - 250.5) * 99 / 99.9
    elif x > 350.4:
        return 401 + (x - 350.5) * 99 / 149.9
    else:
        return None
    
def get_aqi_10(x):
        if x <= 54:
            return x * 50/54
        elif x <= 154:
            return 51 + (x - 55) * 49 / 99
        elif x <= 254:
            return 101 + (x - 155) * 49 / 99
        elif x <= 354:
            return 151 + (x - 255)* 49/99
        elif x <= 424:
            return 201 + (x - 355) * 99 / 69
        elif x <= 504:
            return 301 + (x - 425) * 99 / 79
        elif x > 504:
            return 401 + (x - 504) * 99 / 100
        else:
            return None

# Call the function to perform the transformation

def fill_station_readings():
    try:
        postgres_engine = create_postgres()
        with create_postgres_session(postgres_engine) as postgres_session:
            transform_raw_data(session=postgres_session)
            # falta: 
            #calculate_aqi(session=postgres_session)
    except Exception as e:
        print('An error occurred: ', e)
    finally:
        postgres_session.close()

