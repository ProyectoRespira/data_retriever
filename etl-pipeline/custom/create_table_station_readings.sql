-- creo que aca deberiamos definir las tres layers de 
-- nuestra medallion architecture. Aca creamos:
-- station_readings_bronze
-- station_readings_silver
-- station_readings_gold

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
    bateria VARCHAR
);

CREATE TABLE IF NOT EXISTS station_readings_silver (
    id SERIAL PRIMARY KEY,
    measurement_id INTEGER,
    station_id INTEGER REFERENCES stations(id),
    date_localtime TIMESTAMP,
    pm2_5 FLOAT,
    pm1 FLOAT,
    pm10 FLOAT,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT
);

CREATE TABLE IF NOT EXISTS station_readings_gold (
    id SERIAL PRIMARY KEY,
    station INTEGER REFERENCES stations(id),
    date_localtime TIMESTAMP,
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
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT
)