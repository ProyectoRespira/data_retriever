-- creo que aca deberiamos definir las tres layers de 
-- nuestra medallion architecture. Aca creamos:
-- station_readings_bronze
-- station_readings_silver
-- station_readings_gold

CREATE TABLE station_readings_raw (
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

CREATE TABLE station_readings (
    id SERIAL PRIMARY KEY,
    station INTEGER REFERENCES stations(id),
    date TIMESTAMP,
    pm1 FLOAT,
    pm2_5 FLOAT,
    pm10 FLOAT,
    pm2_5_avg_6h FLOAT,
    pm2_5_max_6h FLOAT,
    pm2_5_skew_6h FLOAT,
    pm2_5_std_6h FLOAT,
    aqi_pm2_5 FLOAT,
    aqi_pm10 FLOAT,
    level INTEGER,
    aqi_pm2_5_max_24h FLOAT,
    aqi_pm2_5_skew_24h FLOAT,
    aqi_pm2_5_std_24h FLOAT,
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT
);