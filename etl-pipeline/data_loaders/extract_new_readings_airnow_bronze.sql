SELECT  
    bronze.id AS measurement_id,
    bronze.date_utc, 
    bronze.station_id, 
    bronze.pm2_5
FROM    
    airnow_readings_bronze bronze
LEFT JOIN
    airnow_readings_silver silver ON bronze.id = silver.measurement_id
WHERE 
    silver.measurement_id IS NULL;