from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,concat,split
import os


PARENT_DIR = '/home/carl/Code/Hadoop/spark/spark-in-action/code'
WAKE_COUNTY_PATH = 'ch03/data/Restaurants_in_Wake_County_NC.csv'
DURHAM_COUNTY_PATH = 'ch03/data/Restaurants_in_Durham_County_NC.json'


def ingest_wake_county(spark):
    csv_file_path = os.path.join(PARENT_DIR, WAKE_COUNTY_PATH)
    df = spark.read.csv(path=csv_file_path, header=True, inferSchema=True)
    return df

def transform_wake_county(wake_df):
    drop_cols = ['OBJECTID', 'GEOCODESTATUS', 'PERMITID']
    wake_df = wake_df.withColumn('county', lit('Wake')) \
                     .withColumnRenamed('HSISID', 'datasetId') \
                     .withColumnRenamed('NAME', 'name') \
                     .withColumnRenamed('ADDRESS1', 'address1') \
                     .withColumnRenamed('ADDRESS2', 'address2') \
                     .withColumnRenamed('CITY', 'city') \
                     .withColumnRenamed('STATE', 'state') \
                     .withColumnRenamed('POSTALCODE', 'zip') \
                     .withColumnRenamed('PHONENUMBER', 'tel') \
                     .withColumnRenamed('RESTAURANTOPENDATE', 'dateStart') \
                     .withColumn('dateEnd', lit(None)) \
                     .withColumnRenamed('FACILITYTYPE', 'type') \
                     .withColumnRenamed('X', 'geoX') \
                     .withColumnRenamed('Y', 'geoY') \
                     .drop(*drop_cols)
    wake_df = wake_df.withColumn('id',
                concat(col('state'), lit('_'), col('county'), lit('_'), col('datasetId')))
    return wake_df


def ingest_durham_county(spark):
    json_file_path = os.path.join(PARENT_DIR, DURHAM_COUNTY_PATH)
    df = spark.read.json(path=json_file_path)
    return df

def transform_durham_county(durham_df):
    drop_cols = ['fields', 'geometry', 'record_timestamp', 'recordid']
    durham_df =  durham_df.withColumn('county', lit('Durham')) \
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
                          .withColumn('geoY', col('fields.geolocation').getItem(1)) \
                          .drop(*drop_cols)

    durham_df = durham_df.withColumn('id',
                concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))
    return durham_df


def union_dataframes(wake_df, durham_df):
    return True



def get_spark_session():
    return SparkSession.builder \
                       .appName('Union DataFrames') \
                       .master('local') \
                       .getOrCreate()

def run_spark_application():
    spark = get_spark_session()
    wake_df = ingest_wake_county(spark)
    wake_df = transform_wake_county(wake_df)
    durham_df = ingest_durham_county(spark)
    durham_df = transform_durham_county(durham_df)
    union_df = union_dataframes(wake_df, durham_df)
    spark.stop()


run_spark_application()