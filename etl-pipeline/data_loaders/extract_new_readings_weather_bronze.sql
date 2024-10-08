SELECT  
    bronze.id AS measurement_id,
    bronze.date_utc, 
    bronze.station_id AS weather_station, 
    bronze.temperature, 
    bronze.rhum AS humidity, 
    bronze.pres AS pressure, 
    bronze.wspd AS wind_speed, 
    bronze.wdir AS wind_dir
FROM    
    weather_readings_bronze bronze
LEFT JOIN
    weather_readings_silver silver ON bronze.id = silver.measurement_id
WHERE 
    silver.measurement_id IS NULL;