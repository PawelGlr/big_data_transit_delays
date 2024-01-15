CREATE DATABASE silver;

CREATE TABLE IF NOT EXISTS silver.vehicle_position (
    lat FLOAT,
    lon FLOAT,
    time TIMESTAMP,
    lines STRING,
    brigade STRING,
    position_key STRING
) STORED AS ORC
location '/transit_delays/silver/vehicle_position';

CREATE TABLE IF NOT EXISTS silver.flow_segment_data (
    frc STRING,
    current_speed INT,
    free_flow_speed INT,
    current_travel_time INT,
    free_flow_travel_time INT,
    confidence FLOAT,
    road_Closure BOOLEAN,
    coordinates ARRAY<STRUCT<lon: FLOAT, lat: FLOAT>>,
    time TIMESTAMP,
    lon_query FLOAT,
    lat_query FLOAT,
    position_key STRING
) STORED AS ORC
location '/transit_delays/silver/flow_segment_data';
