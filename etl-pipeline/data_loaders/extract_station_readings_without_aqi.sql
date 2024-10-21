WITH min_date_utc AS (
    SELECT MIN(date_utc) AS min_date
    FROM station_readings_gold
    WHERE station_id = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
    AND aqi_pm2_5 IS NULL
)
SELECT
    sr.id,
    sr.station_id,
    sr.date_utc,
    sr.pm2_5,
    sr.pm10,
    sr.aqi_pm2_5,
    sr.aqi_pm10,
    sr.aqi_level,
    CASE 
        -- Mark readings between (min_date - 1 hour) and (min_date - 24 hours) as 1
        WHEN sr.date_utc BETWEEN (md.min_date - INTERVAL '24 hour') AND (md.min_date - INTERVAL '1 hour')
        THEN 1
        ELSE 0
    END AS in_24h_interval
FROM
    station_readings_gold sr
CROSS JOIN
    min_date_utc md
WHERE
    sr.station_id = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
AND
    sr.date_utc >= (md.min_date - INTERVAL '24 hour')  -- Only get records within the 24-hour window
ORDER BY
    sr.station_id, sr.date_utc;