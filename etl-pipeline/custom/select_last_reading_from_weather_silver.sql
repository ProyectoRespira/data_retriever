-- Docs: https://docs.mage.ai/guides/sql-blocks
SELECT silver.*
FROM weather_readings_silver silver
WHERE silver.date_utc = (
    SELECT MAX(s.date_utc)
    FROM weather_readings_silver s
    WHERE s.weather_station = silver.weather_station
);
