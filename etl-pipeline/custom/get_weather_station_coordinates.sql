-- Docs: https://docs.mage.ai/guides/sql-blocks
SELECT ws.latitude, ws.longitude, ws.id as station_id
FROM weather_stations ws;