WITH execution_date AS (
    SELECT id, run_date
    FROM inference_runs
    ORDER BY run_date DESC
    LIMIT 1
)

SELECT
    srg.date_utc,
    srg.station_id,  
    srg.pm1,
    srg.pm2_5, 
    srg.pm10,
    srg.pm2_5_avg_6h,
    srg.pm2_5_max_6h,
    srg.pm2_5_skew_6h,
    srg.pm2_5_std_6h,
    srg.aqi_pm2_5,
    srg.aqi_pm10,
    srg.aqi_level,
    srg.aqi_pm2_5_max_24h,
    srg.aqi_pm2_5_skew_24h,
    srg.aqi_pm2_5_std_24h,   
    rr.pm2_5_region_avg,
    rr.pm2_5_region_max,
    rr.pm2_5_region_skew,
    rr.pm2_5_region_std,
    rr.aqi_region_avg,
    rr.aqi_region_max,
    rr.aqi_region_skew,
    rr.aqi_region_std,
    rr.level_region_max,     
    wr.temperature,
    wr.humidity,
    wr.pressure,
    wr.wind_speed,
    wr.wind_dir_cos,
    wr.wind_dir_sin,
    execution_date.id AS inference_run_id,
    execution_date.run_date AS run_date
FROM
    station_readings_gold srg
INNER JOIN
    region_readings rr
    ON srg.date_utc = rr.date_utc
INNER JOIN
    weather_readings_gold wr
    ON srg.date_utc = wr.date_localtime
CROSS JOIN execution_date
WHERE
    srg.station_id = {{ block_output(parse=lambda data, vars: data[0]["id"]) }}
    AND srg.date_utc BETWEEN execution_date.run_date - INTERVAL '25 HOURS' 
                          AND execution_date.run_date - INTERVAL '1 HOURS'
    AND rr.date_utc BETWEEN execution_date.run_date - INTERVAL '25 HOURS' 
                         AND execution_date.run_date - INTERVAL '1 HOURS'
    AND wr.date_localtime BETWEEN execution_date.run_date - INTERVAL '25 HOURS' 
                             AND execution_date.run_date - INTERVAL '1 HOURS'
ORDER BY
    srg.date_utc ASC;
