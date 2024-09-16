SELECT  
    silver.id,
    silver.date_utc, 
    silver.weather_station, 
    silver.temperature, 
    silver.humidity, 
    silver.pressure, 
    silver.wind_speed, 
    silver.wind_dir
FROM    
    weather_readings_silver silver
LEFT JOIN
    weather_readings_gold gold ON silver.id = gold.measurement_id
WHERE 
    gold.measurement_id IS NULL;