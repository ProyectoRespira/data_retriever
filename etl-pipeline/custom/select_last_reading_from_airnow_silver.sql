-- Docs: https://docs.mage.ai/guides/sql-blocks
SELECT silver.station_id, silver.date_utc, silver.pm2_5
FROM airnow_readings_silver silver
WHERE silver.date_utc = (
    SELECT MAX(s.date_utc)
    FROM airnow_readings_silver s
    WHERE s.station_id = silver.station_id
);