CREATE TABLE IF NOT EXISTS region_readings(
    id SERIAL PRIMARY KEY,
    region VARCHAR REFERENCES regions(region_code),
    pm2_5_region_avg FLOAT,
    pm2_5_region_max FLOAT,
    pm2_5_region_skew FLOAT,
    pm2_5_region_std FLOAT,
    aqi_region_avg FLOAT,
    aqi_region_max FLOAT,
    aqi_region_skew FLOAT,
    aqi_region_std FLOAT,
    level_region_max FLOAT
);