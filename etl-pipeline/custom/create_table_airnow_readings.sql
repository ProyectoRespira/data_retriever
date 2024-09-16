-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE IF NOT EXISTS airnow_readings_bronze(
    id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES stations(id), -- constrain: is_pattern_station == True
    date_utc TIMESTAMP WITH TIME ZONE, -- YYYY-MM-DDTHH:mm
    pm2_5 FLOAT, -- pm2_5
    processed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS airnow_readings_silver(
    id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES stations(id),
    measurement_id INTEGER REFERENCES airnow_readings_bronze(id) NULL, --nullable
    date_utc TIMESTAMP WITH TIME ZONE,
    pm2_5 FLOAT,
    data_source VARCHAR(20) CHECK (data_source IN ('raw', 'interpolated')),
    UNIQUE (station_id, date_utc)
);

-- Note: gold readings go into station_readings_gold