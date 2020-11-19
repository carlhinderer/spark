from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, concat
import os
import json

parent_dir = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
data_path = 'ch03/data/Restaurants_in_Wake_County_NC.csv'
csv_data_path = os.path.join(parent_dir, data_path)


# Create Spark session on local master
spark = SparkSession.builder \
                    .appName("Restaurants in Wake County, NC") \
                    .master("local") \
                    .getOrCreate()

# Read the CSV file
df = spark.read.csv(header=True, inferSchema=True, path=csv_data_path)


# Inspect the schema
schema = df.schema

# Schema as a tree
df.printSchema()

# Schema as a string
print(df.schema)

# Schema as json
schema_as_json = df.schema.json()
parsed_schema_as_json = json.loads(schema_as_json)
print("*** Schema as JSON: {}".format(json.dumps(parsed_schema_as_json, indent=2)))


# Stop the Spark session
spark.stop()