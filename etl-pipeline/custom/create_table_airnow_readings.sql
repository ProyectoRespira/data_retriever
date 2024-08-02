-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE IF NOT EXISTS airnow_readings_bronze(
    id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES stations(id), -- constrain: is_pattern_station == True
    date_utc TIMESTAMP, -- readings from airnow come in UTC
    pm2_5 FLOAT
);

CREATE TABLE IF NOT EXISTS airnow_readings_silver(
    id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES stations(id),
    date TIMESTAMP,
    pm2_5 FLOAT
);

-- Note: gold readings go into station_readings_gold