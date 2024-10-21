-- Docs: https://docs.mage.ai/guides/sql-blocks
-- extract_srs_for_calibration
WITH station_data AS (
    -- Aggregate station_readings_silver data to hourly frequency
    SELECT 
        DATE_TRUNC('hour', srs.date_utc) AS date_utc, 
        AVG(srs.pm2_5) AS pm2_5,
        srs.station_id AS station_id
    FROM station_readings_silver srs
    WHERE srs.station_id = '{{ block_output(parse=lambda data, vars: data[0]["station_id"]) }}'
    AND srs.date_utc BETWEEN 
        (DATE_TRUNC('day', CAST('{{ execution_date }}' AS timestamp)) - INTERVAL '3 months') 
        AND DATE_TRUNC('day', CAST('{{ execution_date }}' AS timestamp))
    GROUP BY DATE_TRUNC('hour', srs.date_utc), srs.station_id
),
pattern_data AS (
    -- Retrieve pattern station data from station_readings_gold
    SELECT 
        DATE_TRUNC('hour', srg.date_utc) AS date_utc, 
        AVG(srg.pm2_5) AS pattern_pm2_5,
        srg.station_id AS pattern_station_id
    FROM station_readings_gold srg
    WHERE srg.station_id = '{{ block_output(parse=lambda data, vars: data[0]["pattern_station_id"]) }}'
    AND srg.date_utc BETWEEN 
        (DATE_TRUNC('day', CAST('{{ execution_date }}' AS timestamp) ) - INTERVAL '3 months') 
        AND DATE_TRUNC('day', CAST('{{ execution_date }}' AS timestamp))
    GROUP BY DATE_TRUNC('hour', srg.date_utc), srg.station_id
),
weather_data AS (
    -- Retrieve humidity data from weather_readings_gold
    SELECT 
        DATE_TRUNC('hour', wrg.date_localtime) AS date_localtime, 
        AVG(wrg.humidity) AS humidity,
        wrg.weather_station AS weather_station_id
    FROM weather_readings_gold wrg
    WHERE wrg.weather_station = '{{ block_output(parse=lambda data, vars: data[0]["weather_station_id"]) }}'
    AND wrg.date_localtime BETWEEN 
        (DATE_TRUNC('day', CAST('{{ execution_date }}' AS timestamp) ) - INTERVAL '3 months') 
        AND DATE_TRUNC('day', CAST('{{ execution_date }}' AS timestamp))
    GROUP BY DATE_TRUNC('hour', wrg.date_localtime), wrg.weather_station
)
-- Combine all data using LEFT JOINs based on date_localtime
SELECT 
    sd.date_utc,
    sd.station_id,
    sd.pm2_5,
    wd.humidity,
    pd.pattern_pm2_5
FROM station_data sd
LEFT JOIN pattern_data pd
    ON sd.date_utc = pd.date_utc -- Match pattern station data by date
LEFT JOIN weather_data wd
    ON sd.date_utc = wd.date_localtime; -- Match weather data by date
