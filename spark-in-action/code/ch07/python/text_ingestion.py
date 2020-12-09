from pyspark.sql import SparkSession
import os

PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
SHAKESPEARE_PATH = 'ch07/data/romeo-juliet-pg1777.txt'
TEXT_FILE_PATH = os.path.join(PARENT_DIR, SHAKESPEARE_PATH)


# Creates a session
spark = SparkSession.builder \
    .appName("Text to Dataframe") \
    .getOrCreate()


# Reads Romeo and Juliet, stores it in a dataframe
df = spark.read.format("text") \
        .load(TEXT_FILE_PATH)


# Shows at most 10 rows from the dataframe
df.show(10)
df.printSchema()


# Stop the session
spark.stop()