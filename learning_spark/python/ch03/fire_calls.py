import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: fire_calls.py <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
        .builder
        .appName("SanFranciscoFireCalls")
        .getOrCreate())

    # Get the fire calls data set file name
    sf_fire_file = sys.argv[1]

    fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                    StructField('UnitID', StringType(), True),
                    StructField('IncidentNumber', IntegerType(), True),
                    StructField('CallType', StringType(), True),
                    StructField('CallDate', StringType(), True),
                    StructField('WatchDate', StringType(), True),
                    StructField('CallFinalDisposition', StringType(), True),
                    StructField('AvailableDtTm', StringType(), True),
                    StructField('Address', StringType(), True),
                    StructField('City', StringType(), True),
                    StructField('Zipcode', IntegerType(), True),
                    StructField('Battalion', StringType(), True),
                    StructField('StationArea', StringType(), True),
                    StructField('Box', StringType(), True),
                    StructField('OriginalPriority', StringType(), True),
                    StructField('Priority', StringType(), True),
                    StructField('FinalPriority', IntegerType(), True),
                    StructField('ALSUnit', BooleanType(), True),
                    StructField('CallTypeGroup', StringType(), True),
                    StructField('NumAlarms', IntegerType(), True),
                    StructField('UnitType', StringType(), True),
                    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                    StructField('FirePreventionDistrict', StringType(), True),
                    StructField('SupervisorDistrict', StringType(), True),
                    StructField('Neighborhood', StringType(), True),
                    StructField('Location', StringType(), True),
                    StructField('RowID', StringType(), True),
                    StructField('Delay', FloatType(), True)])

    fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
