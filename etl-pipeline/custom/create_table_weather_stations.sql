CREATE TABLE IF NOT EXISTS weather_stations (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    region VARCHAR REFERENCES regions(region_code)
);

INSERT INTO weather_stations (
    id, 
    name, 
    latitude, 
    longitude, 
    region
) VALUES
    (1, 'Silvio Pettirossi Airport', '-25.2667', '-57.6333', 'GRAN_ASUNCION');