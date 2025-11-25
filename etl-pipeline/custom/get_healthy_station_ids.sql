-- Docs: https://docs.mage.ai/guides/sql-blocks

WITH execution_date AS (
    SELECT date_trunc('hour', run_date) AS truncated_run_date
    FROM inference_runs
    ORDER BY id DESC
    LIMIT 1
)
SELECT DISTINCT
    hc.station_id AS id
FROM health_checks hc
WHERE 
    hc.run_date = (SELECT ed.truncated_run_date FROM execution_date ed)
    AND hc.is_on = TRUE
    AND hc.station_id NOT IN (
        SELECT s.id
        FROM stations s
        WHERE s.is_pattern_station = TRUE
    );
