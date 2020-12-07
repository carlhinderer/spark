import time
import logging
from pyspark.sql import SparkSession


# Creates a session
t0 = int(round(time.time() * 1000))

spark = SparkSession.builder \
                    .appName("Elasticsearch to Dataframe") \
                    .getOrCreate()


# Read from Elasticsearch
t1 = int(round(time.time() * 1000))

logging.info("Getting a session took: {} ms".format((t1 - t0)))

df = spark.read.format("org.elasticsearch.spark.sql") \
          .option("es.nodes", "localhost") \
          .option("es.port", "9200") \
          .option("es.query", "?q=*") \
          .option("es.read.field.as.array.include", "Inspection_Date") \
          .load("nyc_restaurants")


# Shows only a few records as they are pretty long
t2 = int(round(time.time() * 1000))

logging.info("Init communication and starting to get some results took: {} ms".format((t2 - t1)))

df.show(10)


# Show the schema
t3 = int(round(time.time() * 1000))

logging.info("Showing a few records took: {} ms".format((t3 - t2)))

df.printSchema()


# Print the record count
t4 = int(round(time.time() * 1000))

logging.info("Displaying the schema took: {} ms".format((t4 - t3)))
logging.info("The dataframe contains ${df.count} record(s).")


# Print the number of partitions
t5 = int(round(time.time() * 1000))

logging.info("Counting the number of records took: {} ms".format((t5 - t4)))
logging.info("The dataframe is split over {} partition(s).".format(df.rdd.getNumPartitions()))


# Print the time the partition count took
t6 = int(round(time.time() * 1000))

logging.info("Counting the # of partitions took: {} ms".format((t6 - t5)))


# Stops the session
spark.stop()