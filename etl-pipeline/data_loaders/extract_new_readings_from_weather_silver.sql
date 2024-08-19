SELECT silver.*
FROM weather_readings_silver silver
WHERE silver.date_utc > COALESCE((
    SELECT MAX(gold.date_localtime AT TIME ZONE 'America/Asuncion' AT TIME ZONE 'UTC')
    FROM weather_readings_gold gold
    WHERE gold.weather_station = silver.weather_station
), '1970-01-01 00:00:00'::TIMESTAMP);