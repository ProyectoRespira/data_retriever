WITH calibration_check AS (
    SELECT 
        s.id AS station_id,  
        ps.id AS pattern_station_id,  
        ws.id AS weather_station_id,  
        -- Check if the calibration factor exists within the desired date range.
        CASE 
            WHEN cf.id IS NOT NULL 
                AND (DATE_TRUNC('day', CAST('{{ execution_date }}' AS timestamp)) + INTERVAL '1 hour') 
                    BETWEEN cf.date_start_use AND cf.date_end_use
            THEN true  -- This station doesn't need calibration
            ELSE false  -- This station needs calibration
        END AS has_valid_calibration_factor  
    FROM stations s  
    JOIN stations ps 
    ON s.region_id = ps.region_id  -- Join to pattern station in the same region.
    AND ps.is_pattern_station = true  
    JOIN weather_stations ws 
    ON s.region_id = ws.region  -- Join to weather station in the same region.
    LEFT JOIN calibration_factors cf  -- Left join to 'calibration_factors' to check if calibration exists.
    ON s.id = cf.station_id  
    WHERE s.is_pattern_station = false  AND s.is_station_on = true-- Filter to include only non-pattern stations.
)

-- Main query selecting stations that do not have a valid calibration factor in the specified period.
SELECT DISTINCT station_id, pattern_station_id, weather_station_id  
FROM calibration_check 
WHERE has_valid_calibration_factor = false;  -- Filter to only include stations without a valid calibration factor in the period.
