CREATE TABLE IF NOT EXISTS weather_readings_bronze (
    id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES weather_stations(id),
    date_utc TIMESTAMP WITH TIME ZONE, 
    temperature FLOAT,
    dwpt FLOAT,
    rhum FLOAT,
    prcp FLOAT,
    snow FLOAT,
    wdir FLOAT,
    wspd FLOAT,
    wpgt FLOAT,
    pres FLOAT,
    tsun FLOAT,
    coco FLOAT,
    processed BOOLEAN DEFAULT FALSE
);


CREATE TABLE IF NOT EXISTS weather_readings_silver (
    id SERIAL PRIMARY KEY,
    measurement_id INTEGER REFERENCES weather_readings_bronze(id) NULL,
    weather_station INTEGER REFERENCES weather_stations(id),
    date_utc TIMESTAMP WITH TIME ZONE,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    wind_speed FLOAT,
    wind_dir FLOAT,
    data_source VARCHAR(20) CHECK (data_source IN ('raw', 'interpolated')),
    UNIQUE (weather_station, date_utc)

);

CREATE TABLE IF NOT EXISTS weather_readings_gold (
    id SERIAL PRIMARY KEY,
    measurement_id INTEGER REFERENCES weather_readings_silver(id) NULL,
    weather_station INTEGER REFERENCES weather_stations(id),
    date_localtime TIMESTAMP WITH TIME ZONE,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    wind_speed FLOAT,
    wind_dir_cos FLOAT,
    wind_dir_sin FLOAT,
    UNIQUE (weather_station, date_localtime)
);