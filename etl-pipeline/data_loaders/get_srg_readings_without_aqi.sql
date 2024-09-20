SELECT
    sr.id,
    sr.station,
    sr.date_utc,
    sr.pm2_5,
    sr.pm10,
    sr.aqi_pm2_5,
    sr.aqi_pm10,
    sr.aqi_level,
    CASE 
        WHEN sr.date_utc < (
            SELECT MIN(date_utc)
            FROM station_readings_gold
            WHERE aqi_pm2_5 IS NULL
            AND station = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
        ) 
        AND sr.date_utc >= (
            SELECT MIN(date_utc)
            FROM station_readings_gold
            WHERE aqi_pm2_5 IS NULL
            AND station = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
        ) - interval '24 hour'
        THEN 1  -- Flag for readings in the 24-hour interval
        ELSE 0  -- Flag for readings outside the 24-hour interval
    END AS in_24h_interval
FROM
    station_readings_gold sr
WHERE
    sr.station = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
ORDER BY
    sr.station, sr.date_utc;