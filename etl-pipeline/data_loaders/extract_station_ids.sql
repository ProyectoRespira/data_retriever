SELECT 
    id
FROM 
    stations
WHERE 
    is_pattern_station = FALSE
    AND is_station_on = TRUE;