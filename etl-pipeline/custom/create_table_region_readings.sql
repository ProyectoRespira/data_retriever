CREATE TABLE IF NOT EXISTS region_readings(
    id SERIAL PRIMARY KEY,
    date_utc TIMESTAMP WITH TIME ZONE,
    region INTEGER REFERENCES regions(id),
    pm2_5_region_avg FLOAT,
    pm2_5_region_max FLOAT,
    pm2_5_region_skew FLOAT,
    pm2_5_region_std FLOAT,
    aqi_region_avg FLOAT,
    aqi_region_max FLOAT,
    aqi_region_skew FLOAT,
    aqi_region_std FLOAT,
    level_region_max FLOAT,
    UNIQUE (region, date_utc)
);

-- CREATE TABLE IF NOT EXISTS station_readings_gold_to_region (
--     id SERIAL PRIMARY KEY,
--     gold_id INTEGER REFERENCES station_readings_gold(id),
--     region_readings_id INTEGER REFERENCES region_readings(id),
--     date_localtime TIMESTAMP WITH TIME ZONE
-- );
