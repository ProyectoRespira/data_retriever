SELECT *
FROM station_readings_bronze
WHERE station_id = '{{ block_output(parse=lambda data, vars: data[0]["station_id"]) }}' AND
measurement_id > '{{ block_output(parse=lambda data, _vars: data[0]["last_measurement_id"]) }}';