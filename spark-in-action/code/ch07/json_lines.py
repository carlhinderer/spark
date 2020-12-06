from pyspark.sql import SparkSession
import os

PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
FORCLOSURE_PATH = "ch07/data/durham-nc-foreclosure-2006-2016.json"
JSON_FILE_PATH = os.path.join(PARENT_DIR, FORCLOSURE_PATH)

# Creates a session on a local master
spark = SparkSession.builder.appName("JSON Lines to Dataframe").getOrCreate()


df = spark.read.format("json") \
        .load(JSON_FILE_PATH)

# Shows at most 5 rows from the dataframe
df.show(5)

df.printSchema()

spark.stop()