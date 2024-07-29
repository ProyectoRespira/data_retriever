WITH last_measurement AS (
    SELECT 
        max(measurement_id) as last_measurement_id
    FROM 
        station_readings_raw
    WHERE 
        id = '{{ block_output("extract_station_ids")["id"] }}'
)
SELECT
    '{{ block_output("extract_station_ids")["id"] }}' as id,
    CASE 
        WHEN last_measurement_id IS NULL THEN floor(random() * 9 + 1)::int
        ELSE last_measurement_id
    END as last_measurement_id
FROM 
    last_measurement;

-- SELECT 
--     max(measurement_id) as last_measurement_id
-- FROM station_readings_raw
-- WHERE id = '{{ block_output("extract_station_ids")[0]["id"] }}'