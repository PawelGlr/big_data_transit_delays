CREATE DATABASE gold;

CREATE TABLE gold.joined_fact (
    lat FLOAT,
    lon FLOAT,
    time TIMESTAMP,
    lines STRING,
    brigade STRING,
    frc STRING,
    current_speed INT,
    free_flow_speed INT,
    current_travel_time INT,
    free_flow_travel_time INT,
    confidence FLOAT,
    road_closure BOOLEAN,
    coordinates ARRAY<STRUCT<lon: FLOAT, lat: FLOAT>>,
    flow_segment_time TIMESTAMP,
    lon_query FLOAT,
    lat_query FLOAT,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT,
    day_of_week INT,
    day_of_year INT,
    is_weekend BOOLEAN,
    weekofyear INT,
    driven DOUBLE,
    time_diff BIGINT,
    speed DOUBLE
) STORED AS ORC
location '/transit_delays/gold/joined_fact';

CREATE TABLE IF NOT EXISTS gold.hour_aggregated_fact (
    lines STRING,
    brigade STRING,
    year INT,
    month INT,
    day INT,
    hour INT,
    num_observations BIGINT,
    driven_distance DOUBLE,
    average_speed DOUBLE,
    average_road_speed DOUBLE,
    average_free_flow_speed DOUBLE
) STORED AS ORC
LOCATION '/transit_delays/gold/hour_aggregated_fact';

CREATE TABLE IF NOT EXISTS gold.day_aggregated_fact (
    lines STRING,
    brigade STRING,
    year INT,
    month INT,
    day INT,
    num_observations BIGINT,
    driven_distance DOUBLE,
    average_speed DOUBLE,
    average_road_speed DOUBLE,
    average_free_flow_speed DOUBLE
) STORED AS ORC
LOCATION '/transit_delays/gold/day_aggregated_fact';

CREATE TABLE IF NOT EXISTS gold.week_aggregated_fact (
    lines STRING,
    brigade STRING,
    year INT,
    weekofyear INT,
    num_observations BIGINT,
    driven_distance DOUBLE,
    average_speed DOUBLE,
    average_road_speed DOUBLE,
    average_free_flow_speed DOUBLE
) STORED AS ORC
LOCATION '/transit_delays/gold/week_aggregated_fact';