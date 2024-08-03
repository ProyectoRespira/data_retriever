-- Docs: https://docs.mage.ai/guides/sql-blocks
SELECT COUNT(id)
FROM StationReadings
WHERE station = :station_id;