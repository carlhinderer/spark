import logging
from pyspark.sql import SparkSession

# Creates a session on a local master
spark = SparkSession.builder \
                    .appName("MySQL to Dataframe using a JDBC Connection") \
                    .getOrCreate()


# Database settings
mysql_long_url = """
        jdbc:mysql://localhost:3306/sakila?user=root
        &password=Spark<3Java
        &useSSL=false
        &serverTimezone=EST
    """

dbtable = "actor"


# Ingest the actor table from the database
df = spark.read.format("jdbc") \
    .options(url=mysql_long_url,
             dbtable=dbtable)  \
    .load()

df = df.orderBy(df.col("last_name"))


# Displays the dataframe and some of its metadata
df.show(5)
df.printSchema()


# Log information
logging.info("The dataframe contains {} record(s).".format(df.count()))


# Stop the session
spark.stop()