CREATE TABLE IF NOT EXISTS weather_readings_gold (
    id SERIAL PRIMARY KEY,
    weather_station INTEGER REFERENCES weather_stations(id),
    date_localtime TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    wind_speed FLOAT,
    wind_dir_cos FLOAT,
    wind_dir_sin FLOAT
);

CREATE TABLE IF NOT EXISTS weather_readings_silver (
    id SERIAL PRIMARY KEY,
    weather_station INTEGER REFERENCES weather_stations(id),
    date_utc TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    wind_speed FLOAT,
    wind_dir FLOAT
);