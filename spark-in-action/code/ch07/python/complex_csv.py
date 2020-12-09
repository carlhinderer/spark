from pyspark.sql import SparkSession
import os

PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
BOOKS_PATH = 'ch07/data/books.csv'
CSV_FILE_PATH = os.path.join(PARENT_DIR, BOOKS_PATH)


# Creates a session on a local master
spark = SparkSession.builder.appName("Complex CSV to Dataframe").getOrCreate()


# Reads a CSV file with header, called books.csv, stores it in a dataframe
df = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load(CSV_FILE_PATH)


print("Excerpt of the dataframe content:")

# Shows at most 7 rows from the dataframe, with columns as wide as 90 characters
df.show(7, 90)
print("Dataframe's schema:")
df.printSchema()


# Stop session
spark.stop()