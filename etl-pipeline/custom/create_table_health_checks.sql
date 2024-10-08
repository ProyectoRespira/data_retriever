-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE IF NOT EXISTS health_checks (
    id SERIAL PRIMARY KEY,
    run_date TIMESTAMP WITH TIME ZONE,
    station_id INTEGER REFERENCES stations(id),
    last_reading_id INTEGER REFERENCES station_readings_gold(id),
    is_on BOOLEAN
)