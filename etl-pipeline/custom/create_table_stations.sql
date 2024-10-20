CREATE TABLE IF NOT EXISTS stations (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    region_id INTEGER REFERENCES regions(id),
    is_station_on BOOLEAN,
    is_pattern_station BOOLEAN
);


-- normal stations
INSERT INTO stations (
    id, 
    name, 
    latitude, 
    longitude, 
    region_id, 
    is_station_on, 
    is_pattern_station
) VALUES
    (1, 'Campus de la UNA', '-25.33360102213910', '-57.5139365997165', 1, TRUE, FALSE),
    (2, 'Zona Multiplaza', '-25.32014521770180', '-57.56050041876730', 1, TRUE, FALSE),
    (3, 'Acceso Sur', '-25.34024024382230', '-57.58431466296320', 1, TRUE, FALSE),
    (4, 'Primero de Marzo y Perón', '-25.32836979255080', '-57.62706899084150', 1, TRUE, FALSE),
    (5, 'Villa Morra', '-25.29511316679420', '-57.57708610966800', 1, TRUE, FALSE),
    (6, 'Barrio Jara', '-25.28833455406130', '-57.60329900309440', 1, TRUE, FALSE),
    (7, 'San Roque', '-25.28936695307490', '-57.62515967711810', 1, TRUE, FALSE),
    (8, 'Centro de Asunción', '-25.28640403412280', '-57.64701121486720', 1, TRUE, FALSE),
    (9, 'Ñu Guasu', '-25.26458493433890', '-57.54793468862770', 1, TRUE, FALSE),
    (10, 'Botánico', '-25.24647398851810', '-57.54928501322870', 1, TRUE, FALSE)
    ;


-- pattern station
INSERT INTO stations (
    id, 
    name, 
    latitude, 
    longitude, 
    region_id, 
    is_station_on, 
    is_pattern_station
) VALUES
    (101, 'US Embassy - Asunción', '-25.296368', '-57.604671', 1, TRUE, TRUE)
