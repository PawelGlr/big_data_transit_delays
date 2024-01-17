import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("ETL_silver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

vehicle_position_df = spark.read.table("bronze.vehicle_position")

vehicle_position_df = vehicle_position_df.select(
    F.col("lon").cast("float"),
    F.col("lat").cast("float"),
    F.to_timestamp("time", format="yyyy-MM-dd HH:mm:ss").alias("time"),
    F.col("lines"),
    "brigade",
    F.concat(
        F.round("lon", 2),
        F.round("lat", 2),
        F.date_format(F.to_timestamp("time", format="yyyy-MM-dd HH:mm:ss"), "yyyyMMddHH")
    ).alias("position_key")
)

vehicle_position_df.write.format("orc").mode("overwrite").save("/transit_delays/silver/vehicle_position")

flow_segment_data_df = spark.read.table("bronze.flow_segment_data")

flow_segment_data_df = flow_segment_data_df.select(
    "frc",
    F.col("current_Speed").cast("int").alias("current_speed"),
    F.col("free_Flow_Speed").cast("int").alias("free_flow_speed"),
    F.col("current_Travel_Time").cast("int").alias("current_travel_time"),
    F.col("free_Flow_Travel_Time").cast("int").alias("free_flow_travel_time"),
    F.col("confidence").cast("int"),
    F.when(F.col("road_Closure") == "true", True).otherwise(False).alias("road_closure"),
    # coordinates ARRAY<STRUCT<lon: FLOAT, lat: FLOAT>>,
    F.to_timestamp("load_timestamp", format="yyyyMMddHHmmssSSS").alias("time"),
    F.col("lon_query").cast("float"),
    F.col("lat_query").cast("float"),
    F.concat(
        F.round("lon_query", 2),
        F.round("lat_query", 2),
        F.date_format(F.to_timestamp("load_timestamp", format="yyyyMMddHHmmssSSS"), "yyyyMMddHH")
    ).alias("position_key")
)

flow_segment_data_df.write.format("orc").mode("overwrite").save("/transit_delays/silver/flow_segment_data")
