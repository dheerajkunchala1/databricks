# Databricks notebook source
from pyspark import SparkConf,SparkContext
import collections

# COMMAND ----------

conf = SparkConf().setAppName("RatingsHistogram")

# COMMAND ----------

sc = SparkContext(conf=conf)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from covid_ecdc.lookup.fakefriends

# COMMAND ----------

df = spark.sql('select * from covid_ecdc.lookup.fakefriends')

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df = df.repartition(10)

# COMMAND ----------

from pyspark.sql.functions import map

# COMMAND ----------

df.rdd.partitionBy(1).collect()

# COMMAND ----------

from pyspark.sql.functions import length

# COMMAND ----------

for i, partition in enumerate(df):
    print(f"Partition {i}: {len(partition)} rows")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Create a DataFrame
#data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
#columns = ["Name", "Age"]
#df = spark.createDataFrame(data, columns)

# Example that triggers the error
try:
    length_of_column = len(df['_c1'])  # This will raise TypeError
except TypeError as e:
    print(f"TypeError: {e}")


# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Create a DataFrame
#data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("David", 40), ("Eva", 28)]
#columns = ["Name", "Age"]
#df = spark.createDataFrame(data, columns)

# Function to operate on each partition
def process_partition(iter):
    yield [row.Name.upper() for row in iter]

# Apply the function to each partition using mapPartitions
result_rdd = df.rdd.mapPartitions(process_partition)

# Collect the results to the driver program
result_list = result_rdd.collect()

# Print the results
# for i, partition_result in enumerate(result_list):
   # print(f"Partition {i}: {partition_result}")


# COMMAND ----------

def inspect_partition(iter):
    partition_data = list(iter)
    print("Partition data:", partition_data)

# Apply the function to each partition
df.foreachPartition(inspect_partition)

# COMMAND ----------

df.foreachPartition(inspect_partition)

# COMMAND ----------

def get_partition_length(iter):
    partition_data = list(iter)
    return [len(partition_data)]

# Apply the function using mapPartitions and collect the results
partition_lengths = df.rdd.mapPartitions(get_partition_length).collect()

# Display the results
for partition_num, length in enumerate(partition_lengths, start=1):
    print(f"Partition {partition_num}: {length} rows")


# COMMAND ----------


