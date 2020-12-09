import time
from pyspark.sql import SparkSession
from random import random
from operator import add


# Set the number of throws
slices = 10
numberOfThrows = 100000 * slices
print("About to throw {} darts, ready? Stay away from the target!".format(numberOfThrows))

t0 = int(round(time.time() * 1000))


# Create the session
spark = SparkSession.builder.appName("PySpark Pi").getOrCreate()

t1 = int(round(time.time() * 1000))

print("Session initialized in {} ms".format(t1 - t0))


# Build the initial DataFrame
numList = []
for x in range(numberOfThrows):
    numList.append(x)

incrementalRDD = spark.sparkContext.parallelize(numList)

t2 = int(round(time.time() * 1000))
print("Initial dataframe built in {} ms".format(t2 - t1))


# Throw the darts with a function
def throwDarts(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

dartsRDD = incrementalRDD.map(throwDarts)

t3 = int(round(time.time() * 1000))
print("Throwing darts done in {} ms".format(t3 - t2))


# Sum the values of the dart throws
dartsInCircle = dartsRDD.reduce(add)
t4 = int(round(time.time() * 1000))

print("Analyzing result in {} ms".format(t4 - t3))


# Print estimate of Pi
print("Pi is roughly {}".format(4.0 * dartsInCircle/numberOfThrows))


# Stop the session
spark.stop()