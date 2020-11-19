from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

parent_dir = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
data_path = 'ch02/data/authors.csv'
csv_data_path = os.path.join(parent_dir, data_path)


# Create Spark session on local master
spark = SparkSession.builder.appName("CSV to DB").master("local").getOrCreate()

# Read CSV file
df = spark.read.csv(header=True, inferSchema=True, path=csv_data_path)

# Transform data
df = df.withColumn("name", F.concat(F.col("lname"), F.lit(", "), F.col("fname")))

# Save in Postgres DB
db_connection_url = 'jdbc:postgresql://localhost/sparkdb'
db_props = {'driver': 'org.postgresql.Driver', 'user': 'sparkuser', 'password': 'sparkpw'}
df.write.jdbc(mode='append', url=db_connection_url, table='ch02', properties=db_props)

# Stop the Spark session
spark.stop()