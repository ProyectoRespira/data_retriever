SELECT bronze.date_utc, bronze.station_id, bronze.temperature, bronze.rhum, bronze.pres, 
bronze.wspd, bronze.wdir
FROM weather_readings_bronze bronze
WHERE bronze.date_utc > COALESCE((
    SELECT MAX(silver.date_utc)
    FROM weather_readings_silver silver
    WHERE silver.weather_station = bronze.station_id
), '1970-01-01 00:00:00'::TIMESTAMP);