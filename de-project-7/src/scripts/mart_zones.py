# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/mart_zones.py /user/mmakarov/analytics/proj7_repartition/ /user/mmakarov/analytics/proj7/cities/geo.csv /user/mmakarov/prod/zones_mart/

import sys
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

global pi, coef_deg_rad 
pi = 3.14159265359
coef_deg_rad = pi/180


#import findspark
#findspark.init()
#findspark.find()
#import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from pyspark.sql.types import DateType

def main() -> None:
    # input
    events_path = sys.argv[1]
    cities_data_path = sys.argv[2]
    output_path = sys.argv[3]
    #events_path = "/user/mmakarov/analytics/proj7_repartition/"
    #events_path = "/user/master/data/geo/events/"
    #cities_data_path = "/user/mmakarov/analytics/proj7/cities/geo.csv"
    #output_path = "/user/mmakarov/prod/zones_mart/"
    # init session
    #spark = SparkSession.builder \
    #                .master("local") \
    #                .appName("proj7_mm") \
    #                .getOrCreate()
    conf = SparkConf().setAppName(f"mart_zones")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    # perform calculations
    cities_df = cities(cities_data_path, sql)
    events_filtered_df = events_filtered(events_path, sql)
    events_with_geo_df = events_with_geo(events_filtered_df, cities_df)  
    mart_df = mart_zones(events_with_geo_df)
    write = writer(mart_df, output_path)  

    return write
    

def cities(cities_data_path: str, sql) -> DataFrame:
    # read df, convert degrees to radians, drop unnecessary columns
    # input: id, city, lat, lng
    # output: id, city, lat_n_rad, lng_n_rad
    cities_df = (sql.read.option("header", True)
            .option("delimiter", ";")
            .csv(f'{cities_data_path}')
            .withColumn('lat_n', F.regexp_replace('lat', ',' , '.').cast('float'))
            .withColumn('lng_n', F.regexp_replace('lng', ',' , '.').cast('float'))
            .withColumn('lat_n_rad',F.col('lat_n')*F.lit(coef_deg_rad))
            .withColumn('lng_n_rad',F.col('lng_n')*F.lit(coef_deg_rad))
            .drop("lat","lng","lat_n","lng_n")
            .persist()
            )
    return cities_df

def events_filtered(events_path: str, sql) -> DataFrame:
    # readdf, select part of data to speed up,
    # convert degrees to radians, drop unnecessary columns
    events_filtered = (sql
                  .read.parquet(f'{events_path}')
                  #.where('event_type = "message"')
                  #.where('date >= "2022-05-01" and date <= "2022-05-01"')
                  #.select("event.message_id", "event.message_from", "lat", "lon", "date")
                  .withColumn("msg_lat_rad",F.col('lat')*F.lit(coef_deg_rad))
                  .withColumn('msg_lng_rad',F.col('lon')*F.lit(coef_deg_rad))
                  .where('msg_lat_rad IS NOT NULL and msg_lng_rad IS NOT NULL')
                  .drop("lat","lon")
                  .persist()
                  )
    return events_filtered

def events_with_geo(events_clean: DataFrame, cities_clean: DataFrame) -> DataFrame:
    events_with_geo_df = (
        events_clean
        .crossJoin(cities_clean)
        .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.col('msg_lat_rad') - F.col('lat_n_rad'))/F.lit(2)),2)
            + F.cos(F.col("lat_n_rad"))*F.cos(F.col("msg_lat_rad"))*
            F.pow(F.sin((F.col('msg_lng_rad') - F.col('lng_n_rad'))/F.lit(2)),2)
        )))
        .drop("msg_lat_rad","msg_lng_rad", "lat_n_rad", "lng_n_rad"))
    window = Window().partitionBy('event.message_id').orderBy(F.col('distance').asc())
    events_with_geo_df = (
        events_with_geo_df
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col('row_number')==1)
        .drop('row_number', 'distance')
        .withColumn('event_id', F.monotonically_increasing_id())
        .selectExpr("event.message_from as user_id","event_id", "event_type", "id as zone_id", "city", "date")
        .persist()
        )
    return events_with_geo_df

def mart_zones(events_with_geo_df: DataFrame) -> DataFrame:
    window = Window().partitionBy('user_id').orderBy(F.col('date').asc())
    w_month = Window.partitionBy(['zone_id', F.trunc(F.col("date"), "month")])
    w_week = Window.partitionBy(['zone_id', F.trunc(F.col("date"), "week")])

    df_registrations = (
        events_with_geo_df
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col('row_number')==1)
        .drop('row_number')
        .withColumn("month",F.trunc(F.col("date"), "month"))
        .withColumn("week",F.trunc(F.col("date"), "week"))
        .withColumn("week_user", F.count('user_id').over(w_week))
        .withColumn("month_user", F.count('user_id').over(w_month))
        .selectExpr("month","week", "week_user", "month_user")
        .distinct()
        )

    df = (events_with_geo_df
          .withColumn("month",F.trunc(F.col("date"), "month"))
          .withColumn("week",F.trunc(F.col("date"), "week"))
          .withColumn("week_message",F.sum(F.when(events_with_geo_df.event_type == "message",1).otherwise(0)).over(w_week))
          .withColumn("week_reaction",F.sum(F.when(events_with_geo_df.event_type == "reaction",1).otherwise(0)).over(w_week))
          .withColumn("week_subscription",F.sum(F.when(events_with_geo_df.event_type == "subscription",1).otherwise(0)).over(w_week))
          .withColumn("month_message",F.sum(F.when(events_with_geo_df.event_type == "message",1).otherwise(0)).over(w_month))
          .withColumn("month_reaction",F.sum(F.when(events_with_geo_df.event_type == "reaction",1).otherwise(0)).over(w_month))
          .withColumn("month_subscription",F.sum(F.when(events_with_geo_df.event_type == "subscription",1).otherwise(0)).over(w_month))
          .join(df_registrations, ["month", "week"], "fullouter")
          .select("month", "week", "zone_id", "week_message", "week_reaction", "week_subscription", "week_user", "month_message", "month_reaction", "month_subscription", "month_user")
          .distinct()
          )
    return df

    
def writer(df, output_path):
    return df \
        .write \
        .mode('overwrite') \
        .parquet(f'{output_path}')

if __name__ == "__main__":
        main()