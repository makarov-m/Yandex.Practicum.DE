# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/mart_geo_activity.py /user/mmakarov/analytics/proj7_repartition/ /user/mmakarov/analytics/proj7/cities/geo.csv /user/mmakarov/prod/geo_activity_mart/


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
    # events_path = "/user/mmakarov/analytics/proj7_repartition/"
    # events_path = "/user/master/data/geo/events/"
    # cities_data_path = "/user/mmakarov/analytics/proj7/cities/geo.csv"
    # output_path = "/user/mmakarov/analytics/d_marts/"

    # init session
    #spark = SparkSession.builder \
    #                .master("local") \
    #                .appName("proj7_mm") \
    #                .getOrCreate()
    conf = SparkConf().setAppName(f"mart_geo_activity")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    cities_df = cities(cities_data_path, sql)
    events_filtered_df_from = events_filtered_from(events_path, sql)
    events_filtered_df_to = events_filtered_to(events_path, sql)
    events_subscriptions_df = events_subscriptions(events_path, sql)
    events_union_sender_reciever_df = events_union_sender_reciever(events_path, sql)
    combine_dfs = combine_df(events_filtered_df_from, events_filtered_df_to, cities_df, events_subscriptions_df, events_union_sender_reciever_df)
    write = writer(combine_dfs, output_path)  

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

def events_filtered_from(events_path: str, sql) -> DataFrame:
    # readdf, select part of data to speed up,
    # convert degrees to radians, drop unnecessary columns
    events_filtered_from = (sql
                  .read.parquet(f'{events_path}')
                  .where('event_type = "message"')
                  #.where('date >= "2022-05-01" and date <= "2022-05-01"')
                  .selectExpr("event.message_id as message_id_from", "event.message_from", 
                          "event.subscription_channel","lat", "lon", "date")
                  .withColumn("msg_lat_rad_from",F.col('lat')*F.lit(coef_deg_rad))
                  .withColumn('msg_lng_rad_from',F.col('lon')*F.lit(coef_deg_rad))
                  .where('msg_lat_rad_from IS NOT NULL and msg_lng_rad_from IS NOT NULL')
                  .drop("lat","lon")
                  .where("message_from IS NOT NULL")
                  #.where("subscription_channel IS NOT NULL")
                  .persist()
                  )
    
    window = Window().partitionBy('message_from').orderBy(F.col('date').desc())

    events_filtered_from = (
        events_filtered_from
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col('row_number')==1)
        .drop("row_number")
        .persist()
    )

    return events_filtered_from

def events_filtered_to(events_path: str, sql) -> DataFrame:
    # readdf, select part of data to speed up,
    # convert degrees to radians, drop unnecessary columns
    events_filtered_to = (sql
                  .read.parquet(f'{events_path}')
                  .where('event_type = "message"')
                  #.where('date >= "2022-05-01" and date <= "2022-05-01"')
                  .selectExpr("event.message_id as message_id_to", "event.message_to", 
                          "event.subscription_channel","lat", "lon", "date")
                  .withColumn("msg_lat_rad_to",F.col('lat')*F.lit(coef_deg_rad))
                  .withColumn('msg_lng_rad_to',F.col('lon')*F.lit(coef_deg_rad))
                  .where('msg_lat_rad_to IS NOT NULL and msg_lng_rad_to IS NOT NULL')
                  .drop("lat","lon")
                  .where("message_to IS NOT NULL")
                  #.where("subscription_channel IS NOT NULL")
                  .persist()
                  )
    
    window = Window().partitionBy('message_to').orderBy(F.col('date').desc())

    events_filtered_to = (
        events_filtered_to
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col('row_number')==1)
        .drop("row_number")
    )
    return events_filtered_to

def events_subscriptions(events_path: str, sql) -> DataFrame:
    # output: user, list of channels for the user
    events_subscription = (sql
                  .read.parquet(f'{events_path}')
                  .selectExpr('event.user as user','event.subscription_channel as ch') 
                  .where('user is not null and ch is not null')
                  .groupBy('user').agg(F.collect_list(F.col('ch')).alias('chans'))
                  .persist()
                  )
    
    return events_subscription

def events_union_sender_reciever(events_path: str, sql) -> DataFrame:
    # output: user, list of channels for the user
    df_sender_reciever = (sql
            .read.parquet(f'{events_path}')
            .selectExpr('event.message_from as sender','event.message_to as reciever') 
            .where('sender is not null and reciever is not null')
            )
    
    df_reciever_sender = (sql
        .read.parquet(f'{events_path}')
        .selectExpr('event.message_to as reciever','event.message_from as sender') 
        .where('sender is not null and reciever is not null')
        )
    
    union_dfs = (
        df_sender_reciever
        .union(df_reciever_sender)
        .distinct()
        )
    
    # all uniq combinations sender-reciever (users already know each other)
    union_dfs = (union_dfs
                 .withColumn('sender_reciever_existing', F.concat(union_dfs.sender, F.lit("-") , union_dfs.reciever))
                 .drop('sender', 'reciever')
                 )
    
    return union_dfs

def combine_df(events_filtered_from: DataFrame, events_filtered_to: DataFrame, cities_df: DataFrame, events_subscription: DataFrame, union_dfs: DataFrame) -> DataFrame:
    # find all combinations sender and reciever, calculate distance
    result = (
        events_filtered_from
        .crossJoin(events_filtered_to)
        #.where("message_id_from <> message_id_to")
        .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.col('msg_lat_rad_from') - F.col('msg_lat_rad_to'))/F.lit(2)),2)
            + F.cos(F.col("msg_lat_rad_from"))*F.cos(F.col("msg_lat_rad_to"))*
            F.pow(F.sin((F.col('msg_lng_rad_from') - F.col('msg_lng_rad_to'))/F.lit(2)),2)
        )))
        .where("distance <= 1")
        .withColumn("middle_point_lat_rad", (F.col('msg_lat_rad_from') + F.col('msg_lat_rad_to'))/F.lit(2))
        .withColumn("middle_point_lng_rad", (F.col('msg_lng_rad_from') + F.col('msg_lng_rad_to'))/F.lit(2))
        .selectExpr("message_id_from as user_left", "message_id_to as user_right",
                    "middle_point_lat_rad", "middle_point_lng_rad")
        .distinct()
        .persist()
        )
    
    # attach all cities
    result = (
        result
        .crossJoin(cities_df)
        .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.col('middle_point_lat_rad') - F.col('lat_n_rad'))/F.lit(2)),2)
            + F.cos(F.col("middle_point_lat_rad"))*F.cos(F.col("lat_n_rad"))*
            F.pow(F.sin((F.col('middle_point_lng_rad') - F.col('lng_n_rad'))/F.lit(2)),2)
        )))
        .select("user_left", "user_right", "id", "city", "distance")
        .persist()
        )
    
    window = Window().partitionBy("user_left", "user_right").orderBy(F.col('distance').asc())

    # find city by minimum distance
    result = (
        result
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col('row_number')==1)
        .drop('row_number', "distance", "id")
        .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('city')))
        .withColumnRenamed("city", "zone_id")
        .withColumn('sender_reciever_all', F.concat(result.user_left, F.lit("-"), result.user_right))
        .persist()
    )

    # check whether users have not met each other
    result = result.join(union_dfs, result.sender_reciever_all == union_dfs.sender_reciever_existing, "leftanti")

    # add subscriptions to the result and filter 
    # where intersections are exists
    result = (
        result
        .join(events_subscription, result.user_left == events_subscription.user, "left")
        .withColumnRenamed('chans', 'chans_left')
        .drop('user')
        .join(events_subscription, result.user_right == events_subscription.user, "left")
        .withColumnRenamed('chans', 'chans_right')
        .drop('user')
        .withColumn('inter_chans', F.array_intersect(F.col('chans_left'), F.col('chans_right')))
        .filter(F.size(F.col("inter_chans"))>1)
        .where("user_left <> user_right")
        .drop("inter_chans", "chans_left", "chans_right", "sender_reciever_all")
        .withColumn("processed_dttm", F.current_timestamp())
        .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('zone_id')))
        #.withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))
        #.drop("timezone")
        .persist()
        )

    return result

def writer(df, output_path):
    return df \
        .write \
        .mode('overwrite') \
        .parquet(f'{output_path}')

if __name__ == "__main__":
        main()