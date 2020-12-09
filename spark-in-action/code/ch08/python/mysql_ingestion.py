import logging
from pyspark.sql import SparkSession


# Creates a session
spark = SparkSession.builder \
                    .appName("MySQL to Dataframe using a JDBC Connection") \
                    .getOrCreate()


# Database connection settings
user = "root"
password = "Spark<3Java"
use_ssl="false"
mysql_url = "jdbc:mysql://localhost:3306/sakila?serverTimezone=EST"
dbtable = "actor"
database="sakila"


# Ingest the actor table from the database
df = spark.read.format("jdbc") \
        .options(url=mysql_url,
                 database=database,
                 dbtable=dbtable,
                 user=user,
                 password=password) \
        .load()

df = df.orderBy(df.col("last_name"))


# Displays the dataframe and some of its metadata
df.show(5)
df.printSchema()


# Log information
logging.info("The dataframe contains {} record(s).".format(df.count()))


# Stop the session
spark.stop()