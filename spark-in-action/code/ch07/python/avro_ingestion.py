import os
from pyspark.sql import SparkSession

PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
WEATHER_PATH = 'ch07/data/weather.avro'
AVRO_FILE_PATH = os.path.join(PARENT_DIR, WEATHER_PATH)


# Create the session
spark = SparkSession.builder.appName("Avro to Dataframe").getOrCreate()


# Reads an Avro file, stores it in a dataframe
df = spark.read.format("avro") \
          .load(AVRO_FILE_PATH)


# Shows at most 10 rows from the dataframe
df.show(10)
df.printSchema()


# Stop the session
spark.stop()