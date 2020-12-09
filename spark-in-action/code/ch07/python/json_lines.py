from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
FORCLOSURE_PATH = 'ch07/data/durham-nc-foreclosure-2006-2016.json'
JSON_FILE_PATH = os.path.join(PARENT_DIR, FORCLOSURE_PATH)


# Creates a session on a local master
spark = SparkSession.builder.appName("JSON Lines to Dataframe").getOrCreate()


# Load the JSON Lines file
df = spark.read.format("json") \
        .load(JSON_FILE_PATH)


# Access the JSON fields directly
new_df = df.withColumn("year", col("fields.year")) \
           .withColumn("coordinates", col("geometry.coordinates"))


# Shows at most 5 rows from the dataframe
df.show(5)
df.printSchema()


# Display the extracted JSON fields
new_df.show(5)
new_df.printSchema()


# Stop the session
spark.stop()