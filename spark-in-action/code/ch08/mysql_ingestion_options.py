import logging
from pyspark.sql import SparkSession

# Creates a session
spark = SparkSession.builder \
                    .appName("MySQL to Dataframe using a JDBC Connection") \
                    .getOrCreate()


# Ingest the actor table, passing in the JDBC settings as read options
df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/sakila") \
    .option("dbtable", "actor") \
    .option("user", "root") \
    .option("password", "Spark<3Java") \
    .option("useSSL", "false") \
    .option("serverTimezone", "EST") \
    .load()

df = df.orderBy(df.col("last_name"))


# Displays the dataframe and some of its metadata
df.show(5)
df.printSchema()


# Log information
logging.info("The dataframe contains {} record(s).".format(df.count()))


# Stop the session
spark.stop()