from pyspark.sql import SparkSession
from pyspark.sql.functions import (lit,col,concat,split)
import os

parent_dir = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
data_path = 'ch03/data/Restaurants_in_Durham_County_NC.json'
json_data_path = os.path.join(parent_dir, data_path)


# Creates a session on a local master
spark = SparkSession.builder \
                    .appName('Restaurants in Durham County, NC') \
                    .master('local') \
                    .getOrCreate()

# Read a JSON file 
df = spark.read.json(json_data_path)

# Show DataFrame after ingestion
print('*** Right after ingestion ***')
df.show(5)
df.printSchema()
print('We have {} records.'.format(df.count))


# Transform the schema
df =  df.withColumn('county', lit('Durham')) \
        .withColumn('datasetId', col('fields.id')) \
        .withColumn('name', col('fields.premise_name')) \
        .withColumn('address1', col('fields.premise_address1')) \
        .withColumn('address2', col('fields.premise_address2')) \
        .withColumn('city', col('fields.premise_city')) \
        .withColumn('state', col('fields.premise_state')) \
        .withColumn('zip', col('fields.premise_zip')) \
        .withColumn('tel', col('fields.premise_phone')) \
        .withColumn('dateStart', col('fields.opening_date')) \
        .withColumn('dateEnd', col('fields.closing_date')) \
        .withColumn('type', split(col('fields.type_description'), " - ").getItem(1)) \
        .withColumn('geoX', col('fields.geolocation').getItem(0)) \
        .withColumn('geoY', col('fields.geolocation').getItem(1))

df = df.withColumn('id', 
                   concat(col('state'), lit('_'), col('county'), lit('_'), col('datasetId')))


# Show DataFrame after schema transformation
print('*** DataFrame transformed ***')
df.select('id', 'state', 'county', 'datasetId').show(5)
df.printSchema()


# Stop Spark session
spark.stop()