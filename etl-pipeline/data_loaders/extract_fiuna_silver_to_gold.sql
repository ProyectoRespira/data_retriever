-- Docs: https://docs.mage.ai/guides/sql-blocks
WITH unprocessed_hours AS (
    -- Get the minimum unprocessed hour for the station
    SELECT MIN(DATE_TRUNC('hour', silver.date_utc)) AS min_unprocessed_hour
    FROM station_readings_silver silver
    WHERE silver.station_id = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
      AND silver.processed_to_gold = FALSE
),
new_silver_readings AS (
    -- Retrieve all data from the minimum unprocessed hour onwards
    SELECT
        silver.id AS silver_id,
        silver.station_id AS station,
        silver.date_utc,
        silver.pm2_5,
        silver.pm1,
        silver.pm10,
        silver.processed_to_gold,
        s.region AS station_region
    FROM
        station_readings_silver silver
    JOIN
        stations s ON silver.station_id = s.id
    WHERE
        silver.station_id = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
        AND DATE_TRUNC('hour', silver.date_utc) >= (SELECT min_unprocessed_hour FROM unprocessed_hours)
),
date_range AS (
    -- Get the date range for the recalculation (from the min unprocessed hour)
    SELECT
        MIN(date_utc) AS min_date,
        MAX(date_utc) AS max_date
    FROM new_silver_readings
),
weather_data AS (
    -- Retrieve weather data only within the relevant date range and matching region
    SELECT 
        w.date_localtime, 
        w.humidity AS weather_humidity,
        w.weather_station AS weather_station_id
    FROM weather_readings_gold w
    JOIN weather_stations ws ON w.weather_station = ws.id
    WHERE ws.region = (SELECT station_region FROM new_silver_readings LIMIT 1)
      AND w.date_localtime BETWEEN (SELECT min_date FROM date_range) AND (SELECT max_date FROM date_range)
),
calibration_data AS (
    -- Get calibration data relevant to the station and date range
    SELECT 
        c.station_id,
        c.calibration_factor,
        c.date_start_use,
        c.date_end_use
    FROM calibration_factors c
    WHERE c.station_id = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
),
final_data AS (
    -- Combine the silver readings with weather data and calibration factors
    SELECT 
        ns.*,
        wd.weather_humidity,
        COALESCE(
            (SELECT c.calibration_factor 
             FROM calibration_data c
             WHERE ns.station = c.station_id
               AND ns.date_utc BETWEEN c.date_start_use AND c.date_end_use
             ORDER BY c.date_start_use DESC
             LIMIT 1),
            1.0) AS calibration_factor -- Default calibration factor if none found
    FROM new_silver_readings ns
    LEFT JOIN weather_data wd 
        ON wd.date_localtime = DATE_TRUNC('hour', ns.date_utc)
)
-- Final result set ordered by date
SELECT *
FROM final_data
ORDER BY date_utc DESC;