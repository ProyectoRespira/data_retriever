WITH unprocessed_readings AS (
    -- Select new data that hasn't been processed yet and exclude pattern stations
    SELECT
        gold.id,
        gold.date_utc,
        gold.station_id,
        gold.pm2_5,
        gold.aqi_pm2_5,
        gold.aqi_level,
        s.region_id
    FROM station_readings_gold gold
    JOIN stations s ON gold.station_id = s.id
    WHERE 
        gold.processed_to_region = FALSE
        AND s.region_id = 1--'GRAN_ASUNCION'
        AND s.is_pattern_station = FALSE
),
existing_readings AS (
    -- Select previously processed data for the same `date_utc` as the unprocessed data, excluding pattern stations
    SELECT
        gold.id,
        gold.date_utc,
        gold.station_id,
        gold.pm2_5,
        gold.aqi_pm2_5,
        gold.aqi_level,
        s.region_id
    FROM station_readings_gold gold
    JOIN stations s ON gold.station_id = s.id
    WHERE 
        gold.processed_to_region = TRUE
        AND s.region_id = 1--'GRAN_ASUNCION'
        AND s.is_pattern_station = FALSE
        AND gold.date_utc IN (
            SELECT DISTINCT unprocessed.date_utc
            FROM unprocessed_readings unprocessed
        )
)
-- Union the new unprocessed readings with the existing processed readings for recalculation
SELECT *
FROM unprocessed_readings
UNION
SELECT *
FROM existing_readings
ORDER BY date_utc ASC;
