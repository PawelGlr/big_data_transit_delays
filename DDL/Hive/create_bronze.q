CREATE DATABASE bronze;

CREATE TABLE IF NOT EXISTS bronze.vehicle_position (
    lat STRING,
    lon STRING,
    time STRING,
    lines STRING,
    brigade STRING,
    load_timestamp STRING
) STORED AS ORC
location '/transit_delays/bronze/vehicle_position';

CREATE TABLE IF NOT EXISTS bronze.flow_segment_data (
    frc STRING,
    current_Speed STRING,
    free_Flow_Speed STRING,
    current_Travel_Time STRING,
    free_Flow_Travel_Time STRING,
    confidence STRING,
    road_Closure STRING,
    coordinates STRING,
    load_timestamp STRING,
    lon_query STRING,
    lat_query STRING
) STORED AS ORC
location '/transit_delays/bronze/flow_segment_data';
