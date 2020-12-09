from pyspark.sql import SparkSession
import os


PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
COUNTRY_TRAVEL_PATH = "ch07/data/countrytravelinfo.json"
JSON_FILE_PATH = os.path.join(PARENT_DIR, COUNTRY_TRAVEL_PATH)


# Creates a session on a local master
spark = SparkSession.builder.appName("Multiline JSON to Dataframe").getOrCreate()


# Reads a Multiline JSON file
df = spark.read.format("json") \
        .option("multiline", True) \
        .load(JSON_FILE_PATH)


# Shows at most 3 rows from the dataframe
df.show(3)
df.printSchema()


# Stop the session
spark.stop()