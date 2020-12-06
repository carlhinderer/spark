import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               IntegerType,DateType,
                               StringType)

PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
BOOKS_PATH = "ch07/data/books.csv"
CSV_FILE_PATH = os.path.join(PARENT_DIR, BOOKS_PATH)


# Creates a session on a local master
spark = SparkSession.builder.appName("Complex CSV with a Schema").getOrCreate()


# Creates the schema
schema = StructType([StructField('id', IntegerType(), False),
                     StructField('authorId', IntegerType(), True),
                     StructField('bookTitle', IntegerType(), False),
                     StructField('releaseDate', DateType(), True),
                     StructField('url', StringType(), False)])


# Reads a CSV file with header, called books.csv, stores it in a dataframe
df = spark.read.format("csv") \
    .option("header", True) \
    .option("multiline", True) \
    .option("sep", ";") \
    .option("dateFormat", "MM/dd/yyyy") \
    .option("quote", "*") \
    .schema(schema) \
    .load(absolute_file_path)


# Shows at most 30 rows from the dataframe
df.show(30, 90, False)
df.printSchema()


# Show schema as json
schemaAsJson = df.schema.json()
parsedSchemaAsJson = json.loads(schemaAsJson)

print("*** Schema as JSON: {}".format(json.dumps(parsedSchemaAsJson, indent=2)))


# Stop the session
spark.stop()