from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, concat
import os

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

# Show file
print('*** Right after ingestion. ***')
df.show(5)
df.printSchema()
print("We have {} records.".format(df.count()))


# Transform the DataFrame
df = df.withColumn('county', lit('Wake')) \
       .withColumnRenamed('HSISID', 'datasetId') \
       .withColumnRenamed('NAME', 'name') \
       .withColumnRenamed('ADDRESS1', 'address1') \
       .withColumnRenamed('ADDRESS2', 'address2') \
       .withColumnRenamed('CITY', 'city') \
       .withColumnRenamed('STATE', 'state') \
       .withColumnRenamed('POSTALCODE', 'zip') \
       .withColumnRenamed('PHONENUMBER', 'tel') \
       .withColumnRenamed('RESTAURANTOPENDATE', 'dateStart') \
       .withColumnRenamed('FACILITYTYPE', 'type') \
       .withColumnRenamed('X', 'geoX') \
       .withColumnRenamed('Y', 'geoY') \
       .drop("OBJECTID", "PERMITID", "GEOCODESTATUS")

df = df.withColumn('id', 
        concat(col('state'), lit('_'), col('county'), lit('_'), col('datasetId')))

# Show updated DataFrame
print('*** DataFrame Transformed ***')
df.show(5)


# Drop columns we don't need for book exercise
columns_to_drop = ['address2', 'zip', 'tel', 'dateStart', 'geoX', 'geoY', 'address1', 'datasetId']
df_used_for_book = df.drop(*columns_to_drop)

# Show with dropped columns
df_used_for_book.show(5, 15)


# Show the schema
df.printSchema()


# Look at partitions
print('*** Looking at partitions ***')
partition_count = df.rdd.getNumPartitions()
print("Partition count before repartition: {}".format(partition_count))

# Repartition
df = df.repartition(4)
print("Partition count after repartition: {}".format(df.rdd.getNumPartitions()))


# Stop the Spark session
spark.stop()