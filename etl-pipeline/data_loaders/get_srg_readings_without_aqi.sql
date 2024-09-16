SELECT
    sr.id,
    sr.station,
    sr.date_localtime,
    sr.pm2_5,
    sr.pm10,
    sr.aqi_pm2_5,
    sr.aqi_pm10,
    sr.aqi_level
FROM
    station_readings_gold sr
WHERE
    sr.station = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
    AND sr.date_localtime >= (
        SELECT MIN(date_localtime)
        FROM station_readings_gold
        WHERE aqi_pm2_5 IS NULL
        AND station = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}  -- Add station condition here
    ) - interval '24 hour'
ORDER BY
    sr.station, sr.date_localtime;