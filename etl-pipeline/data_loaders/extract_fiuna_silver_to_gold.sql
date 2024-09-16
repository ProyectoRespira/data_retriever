-- Docs: https://docs.mage.ai/guides/sql-blocks
WITH new_silver_readings AS (
    SELECT
        silver.id AS silver_id,
        silver.station_id AS station,
        silver.date_localtime,
        silver.pm2_5,
        silver.pm1,
        silver.pm10,
        s.region AS station_region
    FROM
        station_readings_silver silver
    LEFT JOIN
        station_readings_gold_to_silver gts ON silver.id = gts.silver_id
    JOIN
        stations s ON silver.station_id = s.id
    WHERE
        silver.station_id = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
        AND (
            gts.silver_id IS NULL
            OR DATE_TRUNC('hour', silver.date_localtime) IN (
                SELECT DISTINCT DATE_TRUNC('hour', s2.date_localtime)
                FROM station_readings_silver s2
                LEFT JOIN station_readings_gold_to_silver gts2 ON s2.id = gts2.silver_id
                WHERE s2.station_id = silver.station_id
                  AND gts2.silver_id IS NULL
            )
        )
),
date_range AS (
    SELECT
        MIN(date_localtime) AS min_date,
        MAX(date_localtime) AS max_date
    FROM new_silver_readings
),
weather_data AS (
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
    SELECT 
        c.station_id,
        c.calibration_factor,
        c.date_start_use,
        c.date_end_use
    FROM calibration_factors c
    WHERE c.station_id = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
),
final_data AS (
    SELECT 
        ns.*,
        wd.weather_humidity,
        COALESCE(
            (SELECT c.calibration_factor 
             FROM calibration_data c
             WHERE ns.station = c.station_id
               AND ns.date_localtime BETWEEN c.date_start_use AND c.date_end_use
             ORDER BY c.date_start_use DESC
             LIMIT 1),
            1.0) AS calibration_factor -- Use 1.0 if no calibration factor is found
    FROM new_silver_readings ns
    LEFT JOIN weather_data wd 
        ON wd.date_localtime = DATE_TRUNC('hour', ns.date_localtime)  
)
SELECT *
FROM final_data
ORDER BY date_localtime DESC;
