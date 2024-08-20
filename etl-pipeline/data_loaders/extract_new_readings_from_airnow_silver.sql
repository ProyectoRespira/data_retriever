SELECT silver.date_utc, silver.pm2_5, silver.station_id
FROM airnow_readings_silver silver
WHERE silver.date_utc > COALESCE((
    SELECT MAX(gold.date_localtime AT TIME ZONE 'America/Asuncion' AT TIME ZONE 'UTC')
    FROM station_readings_gold gold
    WHERE gold.station IN (
        SELECT s.id
        FROM stations s
        WHERE s.is_pattern_station = TRUE
    )
), '1970-01-01 00:00:00'::TIMESTAMP);