#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, to_json, col, lit, struct, from_unixtime, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from datetime import datetime
from time import sleep

# kafka topic IN and OUT
TOPIC_NAME_OUT = 'project8.mmakarov_out'  
TOPIC_NAME_IN = 'project8.mmakarov_in'


# In[2]:


def spark_init(session_name: str) -> SparkSession:
    # necessary libraries for integration with Spark с Kafka и PostgreSQL
    spark_jars_packages = ",".join(
            [
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
                "org.postgresql:postgresql:42.4.0",
            ]
        )

    # Create a spark session with required libraries in spark_jars_packages for maximum with Kafka and PostgreSQL
    spark = SparkSession.builder \
        .appName(session_name) \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    return spark


# In[3]:


def read_restaurants_stream(spark: SparkSession, TOPIC_NAME_IN: str) -> DataFrame:
    
    # define the schema of the input message for json
    schema = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", LongType()),
        StructField("adv_campaign_datetime_end", LongType()),
        StructField("datetime_created", LongType()),
    ])

    # read from the Kafka topic messages with promotions from restaurants
    restaurant_read_stream_df = (
        spark.readStream 
        .format('kafka') 
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') 
        .option('subscribe', TOPIC_NAME_IN) 
        .option('kafka.security.protocol', 'SASL_SSL') 
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password=\"ltcneltyn\";') 
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') 
        .load()
        .withColumn('value', col('value').cast(StringType()))
        .withColumn('event', from_json(col('value'), schema))
        .selectExpr('event.*')
    )  
    
    return restaurant_read_stream_df


# In[4]:


def read_clients_static(spark: SparkSession) -> DataFrame:
    # subtract all users with restaurant subscriptions
    subscribers_restaurant_df = (
        spark.read
        .format('jdbc')
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de')
        .option('driver', 'org.postgresql.Driver')
        .option('dbtable', 'subscribers_restaurants')
        .option('user', 'student')
        .option('password', 'de-student')
        .load()
    )

    subscribers_restaurant_df = (
        subscribers_restaurant_df
        .dropDuplicates(["client_id", "restaurant_id"])
        )

    return subscribers_restaurant_df


# In[5]:


def join_modify(restaurant_stream_df, clients_static_df) -> DataFrame:
    """
    {
        "restaurant_id":"123e4567-e89b-12d3-a456-426614174000",
        "adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003",
        "adv_campaign_content":"first campaign",
        "adv_campaign_owner":"Ivanov Ivan Ivanovich",
        "adv_campaign_owner_contact":"iiivanov@restaurant.ru",
        "adv_campaign_datetime_start":1659203516,
        "adv_campaign_datetime_end":2659207116,
        "client_id":"023e4567-e89b-12d3-a456-426614174000",
        "datetime_created":1659131516,
        "trigger_datetime_created":1659304828
    }
    """
    # determine the current time in UTC in milliseconds
    # current_timestamp_utc = int(round(datetime.utcnow().timestamp()))
    
    result = (
        restaurant_stream_df
        .withColumn('trigger_datetime_created', current_timestamp())
        .where(
            (col("adv_campaign_datetime_start") < col("trigger_datetime_created")) & 
            (col("adv_campaign_datetime_end") > col("trigger_datetime_created"))
        )
         .withColumn('timestamp',
                    from_unixtime(col('datetime_created'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
        .withWatermark('timestamp', '10 minutes')
        .dropDuplicates(['restaurant_id', 'adv_campaign_id'])
        .drop("timestamp")
        .join(clients_static_df, "restaurant_id", how="inner") 
        .select("restaurant_id", 
                "adv_campaign_id", 
                "adv_campaign_content", 
                "adv_campaign_owner",
                "adv_campaign_owner_contact", 
                "adv_campaign_datetime_start", 
                "adv_campaign_datetime_end",
                "client_id", 
                "datetime_created", 
                "trigger_datetime_created")
    )

    return result
    


# In[6]:


def send_to_kafka(df):
    
    kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
    }
    
    return (df
            .write
            .format("kafka")
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .options(**kafka_security_options)
            .option("topic", TOPIC_NAME_OUT)
            .option("checkpointLocation", "test_query")
            .save()
           )


# In[7]:


def write_local_db(df):
    
    # write to postgres database
    return (
    df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/de") \
        .option('driver', 'org.postgresql.Driver') \
        .option("dbtable", "subscribers_feedback") \
        .option("user", "jovyan") \
        .option("password", "jovyan") \
        .save()
    )


# In[8]:


def write_to_multiple_targets(df, epoch_id):
    # method for writing data to 2 targets: in PostgreSQL for feedback and in Kafka for triggers
    
    # keep the df in memory so we don't have to re-create the df before sending it to Kafka
    df.persist()
    
    # write df to PostgreSQL with feedback field
    postgres_df = df.withColumn("feedback", lit(None).cast(StringType()))
    write_local_db(postgres_df)
      
    # create a df to send to Kafka. Serialization to json.
    kafka_df = (
        df.withColumn("value", 
                          to_json(
                              struct(
                                col('restaurant_id'),
                                col('adv_campaign_id'),
                                col('adv_campaign_content'),
                                col('adv_campaign_owner'),
                                col('adv_campaign_owner_contact'),
                                col('adv_campaign_datetime_start'),
                                col('adv_campaign_datetime_end'),
                                col('client_id'),
                                col('datetime_created'),
                                col('trigger_datetime_created')
                              )
                          )
                     )
    )
        
    # send messages to the resulting Kafka topic without the feedback field
    send_to_kafka(kafka_df)
        
    # clean memory from df
    df.unpersist()


# In[9]:


if __name__ == "__main__":
    spark = spark_init('RestaurantSubscribeStreamingService')
    clients_static_df = read_clients_static(spark)
    restaurants_stream_df = read_restaurants_stream(spark, TOPIC_NAME_IN)
    join_df = join_modify(restaurants_stream_df, clients_static_df)

    query = join_df.writeStream \
        .foreachBatch(write_to_multiple_targets) \
        .trigger(processingTime="1 minute") \
        .start() 
    

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}"
             )
        sleep(30)

    query.awaitTermination()





