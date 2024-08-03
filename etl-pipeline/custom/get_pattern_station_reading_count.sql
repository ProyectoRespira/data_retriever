SELECT s.id AS station_id, 
       MAX(a.date) AS start_date_utc,
       r.bbox 
FROM regions r
LEFT JOIN stations s ON s.region = r.region_code
LEFT JOIN airnow_readings_silver a ON s.id = a.station_id
WHERE s.is_pattern_station = TRUE
GROUP BY s.id, r.bbox;