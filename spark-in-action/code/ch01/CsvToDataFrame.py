from pyspark.sql import SparkSession
import os

parent_dir = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
data_path = 'ch01/data/books.csv'
csv_data_path = os.path.join(parent_dir, data_path)

# Create a session on the local master
session = SparkSession.builder.appName("Csv To DataFrame").master("local[*]").getOrCreate()

# Read the CSV file with header and store it in a DataFrame
df = session.read.csv(header=True, inferSchema=True, path=csv_data_path)

# Show the first 5 rows from the DataFrame
df.show(5)

# Stop the session
session.stop()