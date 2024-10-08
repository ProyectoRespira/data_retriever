CREATE TABLE IF NOT EXISTS station_readings_bronze (
    id SERIAL PRIMARY KEY,
    measurement_id INTEGER,
    station_id INTEGER REFERENCES stations(id),
    fecha VARCHAR,
    hora VARCHAR,
    mp2_5 VARCHAR,
    mp1 VARCHAR,
    mp10 VARCHAR,
    temperatura VARCHAR,
    humedad VARCHAR,
    presion VARCHAR,
    bateria VARCHAR,
    processed_to_silver BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS station_readings_silver (
    id SERIAL PRIMARY KEY,
    measurement_id INTEGER REFERENCES station_readings_bronze(id) NULL,
    station_id INTEGER REFERENCES stations(id),
    date_utc TIMESTAMP WITH TIME ZONE,
    pm2_5 FLOAT,
    pm1 FLOAT,
    pm10 FLOAT,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    data_source VARCHAR(20) CHECK (data_source IN ('raw', 'interpolated')),
    processed_to_gold BOOLEAN DEFAULT FALSE,
    UNIQUE (station_id, date_utc)
    
);

CREATE TABLE IF NOT EXISTS station_readings_gold (
    id SERIAL PRIMARY KEY,
    station INTEGER REFERENCES stations(id),
    airnow_id INTEGER REFERENCES airnow_readings_silver(id) NULL,
    date_utc TIMESTAMP WITH TIME ZONE,
    pm_calibrated BOOLEAN,
    pm1 FLOAT,
    pm2_5 FLOAT,
    pm10 FLOAT,
    pm2_5_avg_6h FLOAT,
    pm2_5_max_6h FLOAT,
    pm2_5_skew_6h FLOAT,
    pm2_5_std_6h FLOAT,
    aqi_pm2_5 FLOAT,
    aqi_pm10 FLOAT,
    aqi_level INTEGER,
    aqi_pm2_5_max_24h FLOAT,
    aqi_pm2_5_skew_24h FLOAT,
    aqi_pm2_5_std_24h FLOAT,
    processed_to_region BOOLEAN DEFAULT FALSE,
    UNIQUE (station, date_utc)
);

-- CREATE TABLE IF NOT EXISTS station_readings_gold_to_silver (
--     id SERIAL PRIMARY KEY,
--     gold_id INTEGER REFERENCES station_readings_gold(id) NULL,
--     silver_id INTEGER REFERENCES station_readings_silver(id),
--     date_localtime TIMESTAMP WITH TIME ZONE
-- );