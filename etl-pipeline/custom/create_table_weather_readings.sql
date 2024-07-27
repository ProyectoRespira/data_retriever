CREATE TABLE IF NOT EXISTS weather_readings (
    id SERIAL PRIMARY KEY,
    weather_station INTEGER REFERENCES weather_stations(id),
    date TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    wind_speed FLOAT,
    wind_dir_cos FLOAT,
    wind_dir_sin FLOAT
);