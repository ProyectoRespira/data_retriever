-- Docs: https://docs.mage.ai/guides/sql-blocks
WITH inference_run_id AS (
    SELECT id
    FROM inference_runs
    WHERE run_date BETWEEN ('{{ execution_date }}'::timestamp - INTERVAL '2 hours') 
                      AND ('{{ execution_date }}'::timestamp + INTERVAL '1 hours')
    ORDER BY run_date DESC
    LIMIT 1
)
SELECT inference_run_id, station_id, forecasts_12h
FROM inference_results
WHERE inference_run_id = (SELECT id FROM inference_run_id);