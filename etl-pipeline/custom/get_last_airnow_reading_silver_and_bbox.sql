SELECT s.id AS station_id, 
       r.bbox 
FROM regions r
LEFT JOIN stations s ON s.region = r.region_code
WHERE s.is_pattern_station = TRUE
GROUP BY s.id, r.bbox;