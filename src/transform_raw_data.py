from sqlalchemy import func, distinct
from datetime import datetime, timedelta
from .models import StationsReadingsRaw, StationReadings, Stations
from src.database import create_postgres, create_postgres_session

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



def transform_raw_data(session):
    
    # Get distinct station IDs
    station_ids = session.query(distinct(StationsReadingsRaw.station_id)).all()

    for station_id in station_ids:
        print(f'populating station_readings table with data from Station {station_id[0]}')
        # Check if StationReadings table has any data for the current station
        if session.query(StationReadings).filter_by(station=station_id[0]).count() == 0:
            # If there is no data for the current station, grab all raw readings for that station
            print('no previous data for this station...')
            raw_readings = session.query(StationsReadingsRaw).filter_by(station_id=station_id[0]).all()
            print('getting all readings from current station...')
        else:
            # If there is data for the current station, get the timestamp of the last transformation
            print('getting last transformation timestamp...')
            last_transformation_timestamp = get_last_transformation_timestamp(session=session, station_id=station_id[0])
            print('last transformation timestamp gotten!')
            # Query only the new raw readings for the current station
            raw_readings = session.query(StationsReadingsRaw).filter(
                StationsReadingsRaw.fecha + ' ' + StationsReadingsRaw.hora > last_transformation_timestamp,
                StationsReadingsRaw.station_id == station_id[0]
            ).all()

        skipped_records_count = 0

        for raw_reading in raw_readings:
            if raw_reading.fecha and raw_reading.hora:
                # Parse date and time
                print('parsing dates...')
                print(raw_reading.fecha)
                print(raw_reading.hora)
                date_string = raw_reading.fecha + ' ' + raw_reading.hora
                date_time = datetime.strptime(date_string, '%d-%m-%Y %H:%M')
                #print('dates have been parsed!')

                # Parse other fields and convert to appropriate types
                try: 
                    pm1 = float(raw_reading.mp1)
                    pm2_5 = float(raw_reading.mp2_5)
                    pm10 = float(raw_reading.mp10)
                    temperature = float(raw_reading.temperatura)
                    humidity = float(raw_reading.humedad)
                    pressure = float(raw_reading.presion)

                    # Apply correction factor for each PM reading
                    corrected_pm1 = apply_correction_factor(station_id[0], pm1, date_time)
                    corrected_pm2_5 = apply_correction_factor(station_id[0], pm2_5, date_time)
                    corrected_pm10 = apply_correction_factor(station_id[0], pm10, date_time)

                    # Query station details
                    station = session.query(Stations).filter_by(id=raw_reading.station_id).first()

                    # Create a new StationReadings object with corrected PM readings
                    reading = StationReadings(
                        station=station.id,
                        date=date_time,
                        pm1=corrected_pm1,
                        pm2_5=corrected_pm2_5,
                        pm10=corrected_pm10,
                        temperature=temperature,
                        humidity=humidity,
                        pressure=pressure,
                        # Set AQI values to None initially
                        aqi_pm2_5=None,
                        aqi_pm10=None
                    )

                # Add the new reading to the session
                    session.add(reading)
                except (ValueError, TypeError) as e:
                    skipped_records_count +=1
                    continue
            else:
                skipped_records_count += 1
        
        print(f'Station {station_id[0]}: Number of records skipped ={skipped_records_count}')
    # Commit the changes to the database
    session.commit()


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
            #calculate_aqi(session=postgres_session)
    except Exception as e:
        print("An error occurred:", e)
