from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import acos, cos, sin, lit, radians

# Create Spark session
spark = SparkSession.builder \
    .appName("ETL_gold") \
    .enableHiveSupport() \
    .getOrCreate()

def get_distance(lon_a, lat_a, lon_b, lat_b):
    lon_a_rad = radians(lon_a)
    lat_a_rad = radians(lat_a)
    lon_b_rad = radians(lon_b)
    lat_b_rad = radians(lat_b)
    distance = acos(
        sin(lat_a_rad) * sin(lat_b_rad) +
        cos(lat_a_rad) * cos(lat_b_rad) * cos(lon_a_rad - lon_b_rad)
    ) * lit(6371.0)  # Earth's radius in kilometers
    return distance


    

vehicle_position_df = spark.read.table("silver.vehicle_position")
flow_segment_data_df = spark.read.table("silver.flow_segment_data").withColumnRenamed("time", "flow_segment_time")

joined_fact_df = vehicle_position_df.join(
    flow_segment_data_df,
    vehicle_position_df.position_key == flow_segment_data_df.position_key,
    "left"
 )

w = Window.partitionBy(["lines", "brigade"]).orderBy("time")

joined_fact_df = (
    joined_fact_df
    #calendar columns
    .withColumn("year", F.year("time"))
    .withColumn("month", F.month("time"))
    .withColumn("day", F.dayofmonth("time"))
    .withColumn("hour", F.hour("time"))
    .withColumn("minute", F.minute("time"))
    .withColumn("day_of_week", F.dayofweek("time")) # 1 = Sunday, 7 = Saturday
    .withColumn("day_of_year", F.dayofyear("time"))
    .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False))
    .withColumn("weekofyear", F.weekofyear("time"))
    #calculate driven distance and speed
    .withColumn("driven", get_distance(F.col("lon"), F.col("lat"), F.lag("lon").over(w), F.lag("lat").over(w)))
    .withColumn("time_diff", F.unix_timestamp("time") - F.unix_timestamp(F.lag("time").over(w)))
    .withColumn("speed", F.col("driven") / F.col("time_diff") * 3.6)
    .drop("position_key")
)

hour_aggregated_fact_df = (
    joined_fact_df
    .groupBy("lines", "brigade", "year", "month", "day", "hour")
    .agg(
        F.count("*").alias("num_observations"),
        F.sum("driven").alias("driven_distance"),
        F.avg("speed").alias("average_speed"),
        F.avg("current_speed").alias("average_road_speed"),
        F.avg("free_flow_speed").alias("average_free_flow_speed"),
    )
)


day_aggregated_fact_df = (
    joined_fact_df
    .groupBy("lines", "brigade", "year", "month", "day")
    .agg(
        F.count("*").alias("num_observations"),
        F.sum("driven").alias("driven_distance"),
        F.avg("speed").alias("average_speed"),
        F.avg("current_speed").alias("average_road_speed"),
        F.avg("free_flow_speed").alias("average_free_flow_speed"),
    )
)

week_aggregated_fact_df = (
    joined_fact_df
    .groupBy("lines", "brigade", "year", "weekofyear")
    .agg(
        F.count("*").alias("num_observations"),
        F.sum("driven").alias("driven_distance"),
        F.avg("speed").alias("average_speed"),
        F.avg("current_speed").alias("average_road_speed"),
        F.avg("free_flow_speed").alias("average_free_flow_speed"),
    )
)


joined_fact_df.write.format("orc").mode("overwrite").save("/transit_delays/gold/joined_fact")
hour_aggregated_fact_df.write.format("orc").mode("overwrite").save("/transit_delays/gold/hour_aggregated_fact")
day_aggregated_fact_df.write.format("orc").mode("overwrite").save("/transit_delays/gold/day_aggregated_fact")
week_aggregated_fact_df.write.format("orc").mode("overwrite").save("/transit_delays/gold/week_aggregated_fact")
