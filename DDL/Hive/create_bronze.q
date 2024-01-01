CREATE DATABASE bronze;

CREATE EXTERNAL TABLE IF NOT EXISTS bronze.vehicle_position (
    Lat STRING,
    Lon STRING,
    Time STRING,
    Lines STRING,
    Brigade STRING,
    load_timestamp STRING
) STORED AS AVRO
location '/transit_delays/bronze/vehicle_position';

CREATE EXTERNAL TABLE IF NOT EXISTS bronze.flow_segment_data (
    frc STRING,
    current_Speed STRING,
    free_Flow_Speed STRING,
    current_Travel_Time STRING,
    free_Flow_Travel_Time STRING,
    confidence STRING,
    road_Closure STRING,
    coordinates STRING,
    load_timestamp STRING
) STORED AS AVRO
location '/transit_delays/bronze/flow_segment_data';
