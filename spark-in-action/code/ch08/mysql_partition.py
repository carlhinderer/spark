import logging
from pyspark.sql import SparkSession


# Creates a session
spark = SparkSession.builder \
                    .appName("MySQL to Dataframe using JDBC without partioning") \
                    .getOrCreate()


# Database settings
user = "root"
password = "Spark<3Java"
use_ssl="false"
mysql_url = "jdbc:mysql://localhost:3306/sakila?serverTimezone=EST"
dbtable = "film"


# Partition settings
partition_column = "film_id"
lower_bound = 1
upper_bound = 1000
num_partitions = 10


# Read from the film table
df = spark.read.format("jdbc") \
    .option("url", mysql_url) \
    .option("user", user) \
    .option("dbtable", dbtable) \
    .option("password", password) \
    .option("partitionColumn", partition_column) \
    .option("lowerBound", lower_bound) \
    .option("upperBound", upper_bound) \
    .option("numPartitions", num_partitions) \
    .load()


# Displays the dataframe and some of its metadata
df.show(5)
df.printSchema()


# Log information
logging.info("The dataframe contains {} record(s).".format(df.count()))
logging.info("The dataframe is split over ${} partition(s).".format(df.rdd.getNumPartitions()))


# Stop the session
spark.stop()