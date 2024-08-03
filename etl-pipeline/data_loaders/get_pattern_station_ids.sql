SELECT s.id AS station_id, COUNT(a.id) AS reading_count
FROM stations s
LEFT JOIN airnow_readings_bronze a
ON s.id = a.station_id
WHERE s.is_pattern_station = TRUE
GROUP BY s.id;