WITH LastMeasurement AS (
    SELECT station_id, MAX(measurement_id) AS last_measurement_id
    FROM station_readings_bronze
    GROUP BY station_id
)
SELECT s.id AS station_id, COALESCE(lm.last_measurement_id, 0) AS last_measurement_id
FROM stations s
LEFT JOIN LastMeasurement lm ON s.id = lm.station_id
WHERE s.is_pattern_station = false AND s.is_station_on = true;