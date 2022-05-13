import pymongo
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType
from pyspark.sql import SparkSession

### Creating a Conection Between Spark And Kafka ####
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
bootstrapServers = "Cnt7-naya-cdh63:9092"
topics = "amadeus"
####  Creating Spark Session ####
spark = SparkSession\
        .builder\
        .appName("amadeus")\
        .getOrCreate()

#### ReadStream From Kafka Amadeus Topic ####
df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option("subscribe", topics)\
    .load()

#### Creating a Schema for Spark Structured Streaming ####
schema = StructType() \
    .add("origin_city", StringType())\
    .add("dest_city", StringType())\
    .add("dep_date", StringType())\
    .add("return_date", StringType())\
    .add("user_id", StringType())\
    .add("chat_id", StringType())\
    .add("user_last_name", StringType())\
    .add("user_user_name", StringType())\
    .add("user_first_name", StringType())\
    .add("carrier", StringType())\
    .add("request_type", StringType())\
    .add("duration", StringType())\
    .add("price_total", StringType())\
    .add("price_currency", StringType())\
    .add("cabin", StringType())

### Change Json To Dataframe With Schema ####
df_kafka = df_kafka.select(col("value").cast("string"))\
    .select(from_json(col("value"), schema).alias("value"))\
    .select("value.*")

#### Adding Calculated Columns To Spark Data Frame ####
df_kafka = df_kafka.withColumn("current_ts", current_timestamp().cast('string'))
df_kafka= df_kafka.withColumn("is_active", lit(1))


#### Defining A Function To Send Dataframe To MongoDB ####
def write_df_mongo(target_df):

    mogodb_client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = mogodb_client["Amadeus_DB"]
    mycol = mydb["Flights_Requests"]
    post = {
        "origin_city": target_df.origin_city,
        "dest_city": target_df.dest_city,
        "dep_date": target_df.dep_date,
        "return_date": target_df.return_date,
        "user_id": target_df.user_id,
        "chat_id": target_df.chat_id,
        "user_last_name": target_df.user_last_name,
        "user_first_name": target_df.user_last_name,
        "user_user_name": target_df.user_last_name,
        "carrier": target_df.carrier,
        "request_type": target_df.request_type,
        "duration": target_df.duration,
        "price_total": target_df.price_total,
        "price_currency": target_df.price_currency,
        "cabin": target_df.cabin,
        "current_ts": target_df.current_ts,
        "is_active": target_df.is_active

    }

    mycol.insert_one(post)
    print('item inserted')
    print(post)

#### Spark Action ###
df_kafka \
    .writeStream \
    .foreach(write_df_mongo)\
    .outputMode("append") \
    .start() \
    .awaitTermination()
df_kafka.show(truncate=False)