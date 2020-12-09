from pyspark.sql import SparkSession
import os

PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
DEMO_PATH = 'ch07/data/demo-11-zlib.orc'
ORC_FILE_PATH = os.path.join(PARENT_DIR, DEMO_PATH)


# Creates a session
spark = SparkSession.builder.appName("ORC to Dataframe") \
    .config("spark.sql.orc.impl", "native") \
    .getOrCreate()


# Reads an ORC file
df = spark.read.format("orc") \
          .load(ORC_FILE_PATH)


# Shows at most 10 rows from the dataframe
df.show(10)
df.printSchema()


# Stop the session
spark.stop()