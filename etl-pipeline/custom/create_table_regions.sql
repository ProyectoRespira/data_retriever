CREATE TABLE IF NOT EXISTS regions (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    region_code VARCHAR UNIQUE,
    bbox VARCHAR,
    has_weather_data BOOLEAN,
    has_pattern_station BOOLEAN
);

INSERT INTO regions (
    name, 
    region_code, 
    bbox, 
    has_weather_data, 
    has_pattern_station
)
VALUES (
    'Gran Asunción', 
    'GRAN_ASUNCION', 
    '-57.680,-25.410,-57.470,-25.140', 
    FALSE, 
    FALSE
);