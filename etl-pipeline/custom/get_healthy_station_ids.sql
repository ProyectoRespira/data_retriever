-- Docs: https://docs.mage.ai/guides/sql-blocks
WITH execution_date AS (
    SELECT run_date
    FROM inference_runs
    ORDER BY id DESC
    LIMIT 1
)

SELECT
    station_id as id
FROM health_checks
WHERE 
    run_date = (SELECT date_trunc('hour', run_date) - INTERVAL '1 HOUR' FROM execution_date)
    AND is_on = TRUE
    AND station_id NOT IN (SELECT id FROM stations WHERE is_pattern_station = TRUE);