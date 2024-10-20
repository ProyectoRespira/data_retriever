INSERT INTO health_checks (run_date, station_id, last_reading_id, is_on)
SELECT
    DATE_TRUNC('hour', '{{ execution_date }}'::timestamp) AS run_date,  -- Truncate to the hour
    s.id AS station_id,
    sr.id AS last_reading_id,
    CASE 
        WHEN sr.date_utc >= DATE_TRUNC('hour', '{{ execution_date }}'::timestamp) - INTERVAL '6 hours' THEN TRUE
        ELSE FALSE
    END AS is_on
FROM
    stations s
LEFT JOIN LATERAL (
    SELECT sr.id, sr.date_utc
    FROM station_readings_gold sr
    WHERE sr.station = s.id
    ORDER BY sr.date_utc DESC
    LIMIT 1
) sr ON TRUE
ORDER BY
    s.id;