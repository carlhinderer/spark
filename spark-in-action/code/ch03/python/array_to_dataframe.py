from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Creates a session on a local master
spark = SparkSession.builder \
                    .appName("Array to Dataframe") \
                    .master("local[*]") \
                    .getOrCreate()

# Sample array data
data = [['Jean'], ['Liz'], ['Pierre'], ['Lauric']]

# Create schema for DataFrame
schema = StructType([StructField('name', StringType(), True)])

"""
* data:    parameter list1, data to create a dataset
* encoder: parameter list2, implicit encoder
"""
df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

# Stop session
spark.stop()