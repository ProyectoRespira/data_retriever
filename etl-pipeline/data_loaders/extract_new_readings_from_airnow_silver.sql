SELECT  
    silver.id AS airnow_id,
    silver.date_utc, 
    silver.station_id AS station, 
    silver.pm2_5
FROM    
    airnow_readings_silver silver
LEFT JOIN
    station_readings_gold gold ON silver.id = gold.airnow_id
JOIN
    stations st ON silver.station_id = st.id
WHERE 
    (gold.airnow_id IS NULL 
    OR (gold.pm2_5 IS NOT NULL AND gold.pm2_5 != silver.pm2_5)) --check for updated pm2_5
    AND st.is_pattern_station = TRUE;
