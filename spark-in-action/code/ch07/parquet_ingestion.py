from pyspark.sql import SparkSession
import os

PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
TYPES_PATH = 'ch07/data/alltypes_plain.parquet'
PARQUET_FILE_PATH = os.path.join(PARENT_DIR, TYPES_PATH)


# Creates a session
spark = SparkSession.builder.appName("Parquet to Dataframe").getOrCreate()


# Reads a Parquet file, stores it in a dataframe
df = spark.read.format("parquet") \
    .load(PARQUET_FILE_PATH)


# Shows at most 10 rows from the dataframe
df.show(10)
df.printSchema()
print("The dataframe has {} rows.".format(df.count()))


# Stop the session
spark.stop()