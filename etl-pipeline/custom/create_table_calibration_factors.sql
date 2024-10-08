CREATE TABLE IF NOT EXISTS calibration_factors(
    id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES stations(id),
    date_start_cal TIMESTAMP,
    date_end_cal TIMESTAMP,
    station_mean FLOAT,
    pattern_mean FLOAT,
    date_start_use TIMESTAMP,
    date_end_use TIMESTAMP,
    calibration_factor FLOAT
);