-- Docs: https://docs.mage.ai/guides/sql-blocks
SELECT bronze.*
FROM airnow_readings_bronze bronze
WHERE bronze.date_utc::TIMESTAMP > COALESCE((
    SELECT MAX(silver.date_utc)
    FROM airnow_readings_silver silver
), '1970-01-01 00:00:00'::TIMESTAMP);