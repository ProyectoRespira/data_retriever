WITH source_data AS (
    SELECT * FROM {{ df_1 }}
)

INSERT INTO airnow_readings_bronze (station_id, utc, longitude, latitude, parameter, unit, value)
SELECT station_id, latitude, longitude, parameter, unit, value
FROM source_data;