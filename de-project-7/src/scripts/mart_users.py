# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/mart_users.py /user/mmakarov/analytics/proj7_repartition/ /user/mmakarov/analytics/proj7/cities/geo.csv /user/mmakarov/prod/user_mart/

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
    # output_path = "/user/mmakarov/prod/user_mart/"
    
    # init session
    #spark = SparkSession.builder \
    #                .master("local") \
    #                .appName("proj7_mm") \
    #                .getOrCreate()
    conf = SparkConf().setAppName(f"mart_users")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    # calculations
    cities_df = cities(cities_data_path, sql)
    events_filtered_df = events_filtered(events_path, sql)
    events_with_geo_df = events_with_geo(events_filtered_df, cities_df)            # STEP 2.1.1.   events connected with cities  
    actual_geo_df = actual_geo(events_with_geo_df)                                 # STEP 2.2.1.   cities calculated for the last message  
    travel_geo_df = travel_geo(events_with_geo_df)                                 # STEP 2.3.3.   number of cities vicited by user
    home_geo_df = home_geo(travel_geo_df)                                          # STEP 2.2.3.   home address
    mart_act_home_df = int_mart_act_home(actual_geo_df, home_geo_df, cities_df)
    mart_users_cities_df = mart_users_cities(events_path, mart_act_home_df, sql)
    write = writer(mart_users_cities_df, output_path)                              # final data mart

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
    # output: event.message_id, event.message_from, msg_lat_rad, msg_lng_rad, date
    events_filtered = (sql
                  .read.parquet(f'{events_path}')
                  .where('event_type = "message"')
                  #.where('date >= "2022-05-01" and date <= "2022-05-01"')
                  .select("event.message_id", "event.message_from", "lat", "lon", "date")
                  .withColumn("msg_lat_rad",F.col('lat')*F.lit(coef_deg_rad))
                  .withColumn('msg_lng_rad',F.col('lon')*F.lit(coef_deg_rad))
                  .drop("lat","lon")
                  .persist()
                  )
    return events_filtered

def events_with_geo(events_clean: DataFrame, cities_clean: DataFrame) -> DataFrame:
    # input: event.message_id, event.message_from, msg_lat_rad, msg_lng_rad, date
    # output: message_id, message_from, date, city, distance
    events_with_geo_df = (
        events_clean
        .crossJoin(cities_clean)
        .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.col('msg_lat_rad') - F.col('lat_n_rad'))/F.lit(2)),2)
            + F.cos(F.col("lat_n_rad"))*F.cos(F.col("msg_lat_rad"))*
            F.pow(F.sin((F.col('msg_lng_rad') - F.col('lng_n_rad'))/F.lit(2)),2)
        )))
        .drop("msg_lat_rad","msg_lng_rad","lat_n_rad", "lng_n_rad")
        )
    window = Window().partitionBy('message_id').orderBy(F.col('distance').asc())
    events_with_geo_df = (
        events_with_geo_df
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col('row_number')==1)
        .drop('row_number')
        .persist()
        )
    return events_with_geo_df

def actual_geo(events_with_geo_df: DataFrame) -> DataFrame:
    # input: message_id, message_from, date, city, distance, +id
    # output: message_id, user_id, city, +id
    window = Window().partitionBy('message_from').orderBy(F.col('date').desc())
    act_city = (events_with_geo_df
            .withColumn("row_number", F.row_number().over(window))
            .filter(F.col('row_number')==1)
            .selectExpr('message_id', 'message_from as user_id', 'city', 'id')
            .persist()
           )
    return act_city

def travel_geo(events_with_geo_df: DataFrame) -> DataFrame:
    # calculates: number of cities visited
    # input columns: message_id, user_id, date, city, distance, +id
    # output columns: user, date_diff, id (city), cnt_city
    window = Window().partitionBy('message_from', 'message_id').orderBy(F.col('date'))
    df_travel = (
        events_with_geo_df
        .withColumn("dense_rank", F.dense_rank().over(window))
        .withColumn("date_diff", F.datediff(
            F.col('date').cast(DateType()), F.to_date(F.col("dense_rank").cast("string"), 'd')
            )
        )
        .selectExpr('date_diff', 'message_from as user', 'date', "message_id", "id")
        .groupBy("user", "date_diff", "id")
        .agg(F.countDistinct(F.col('date')).alias('cnt_city'))
        )
    return df_travel

def home_geo(df_travel: DataFrame) -> DataFrame:
    # input columns: user, date_diff, id (city), cnt_city
    # output columns: user, date_diff, id (city), cnt_city, max_dt
    home_city = (
        df_travel
        .filter((F.col('cnt_city')>27))   
        .withColumn('max_dt', F.max(F.col('date_diff')).over(Window().partitionBy('user'))) 
        .filter(F.col('date_diff') == F.col('max_dt'))       
        .persist()
    )
    return home_city

def int_mart_act_home (act_city: DataFrame, home_city: DataFrame, cities_df: DataFrame) -> DataFrame:
    # output: user_id, act_city, home_city
    cities_df = cities_df.select("id", "city")
    home_city = (
        home_city
        .join(cities_df, home_city.id == cities_df.id, "inner")
        .selectExpr('user', 'city as home_city')
    )

    report = (
       act_city
       .join(home_city, act_city.user_id == home_city.user, "fullouter")
       .selectExpr('user_id', 'city as act_city', "home_city")
    )
    return report

def mart_users_cities(events_path: DataFrame, report: DataFrame, sql) -> DataFrame:
    # calculates: time based on last message of the user
    # output:
    times = (
        sql.read.parquet(f'{events_path}')
        .where('event_type = "message"')
        .selectExpr("event.message_from as user_id", "event.datetime", "event.message_id")
        .where("datetime IS NOT NULL")
    )
    window = Window().partitionBy('user_id').orderBy(F.col('datetime').desc())

    times_w = (times
            .withColumn("row_number", F.row_number().over(window))
            .filter(F.col('row_number')==1)
            .withColumn("TIME",F.col("datetime").cast("Timestamp"))
            .selectExpr("user_id as user", "Time")
    )

    mart = (
        report
        .join(times_w, report.user_id == times_w.user, "left").drop("user")
        .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('act_city')))
        #.withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))
        )
    
    return mart
    
def writer(df, output_path):
    return df \
        .write \
        .mode('overwrite') \
        .parquet(f'{output_path}')

if __name__ == "__main__":
        main()



