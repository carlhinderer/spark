from pyspark.sql import SparkSession
import os

PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
NASA_PATENTS_PATH = 'ch07/data/nasa-patents.xml'
XML_FILE_PATH = os.path.join(PARENT_DIR, NASA_PATENTS_PATH)


# Create the session
spark = SparkSession.builder.appName("XML to Dataframe").getOrCreate()


# Reads an XML file
df = spark.read.format("xml") \
        .option("rowTag", "row") \
        .load(XML_FILE_PATH)


# Shows at most 5 rows from the dataframe
df.show(5)
df.printSchema()


# Stop the session
spark.stop()