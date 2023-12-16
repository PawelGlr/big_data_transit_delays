CREATE DATABASE bronze;


CREATE TABLE IF NOT EXISTS bronze.vehicle_position (
    Lat STRING,
    Lon STRING,
    Time STRING,
    Lines STRING,
    Brigade STRING
) STORED AS AVRO
location '/transit_delays/bronze/vehicle_position';

CREATE TABLE IF NOT EXISTS bronze.flow_segment_data (
    frc STRING,
    currentSpeed STRING,
    freeFlowSpeed STRING,
    currentTravelTime STRING,
    freeFlowTravelTime STRING,
    confidence STRING,
    roadClosure STRING,
    coordinates STRING
) STORED AS AVRO
location '/transit_delays/bronze/flow_segment_data';
