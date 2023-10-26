# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder.appName("StructuredStreamingExample").getOrCreate()

# Define a schema for the incoming JSON data
json_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

# Create a streaming DataFrame by reading from a source (e.g., Kafka, file, or socket)
streaming_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "streaming_topic").load()

# Parse the JSON data from Kafka as a new column "value" and apply the schema
parsed_streaming_df = streaming_df.selectExpr("CAST(value AS STRING) as json_data").select(from_json("json_data", json_schema).alias("data"))

# Select and transform columns using selectExpr
transformed_streaming_df = parsed_streaming_df.selectExpr("data.name as full_name", "data.age + 5 as age_plus_5")

# Define a query to output the transformed streaming data (e.g., to console)
query = transformed_streaming_df.writeStream.outputMode("append").format("console").start()

# Wait for the query to terminate
query.awaitTermination()


# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("SelectExprExample").getOrCreate()

# Sample data
data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 22)]
columns = ["id", "name", "age"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Show the original DataFrame
df.show()

# Using selectExpr to select and manipulate columns
result_df = df.selectExpr("name", "age + 5 as age_plus_5")

# Show the result DataFrame
result_df.show()


# COMMAND ----------

def get_last_checkpoint_offset(checkpoint_file):
    try:
        with open(checkpoint_file, 'r') as file:
            last_checkpoint_offset = int(file.read())
        return last_checkpoint_offset
    except FileNotFoundError:
        # If the checkpoint file doesn't exist, return an initial offset.
        return 0

# Usage:
checkpoint_file = 'kafka_checkpoint.txt'
last_checkpoint_offset = get_last_checkpoint_offset(checkpoint_file)


# COMMAND ----------

from confluent_kafka import Consumer, KafkaError

# Initialize Kafka consumer configuration
conf = {
    'bootstrap.servers': 'your_kafka_broker',
    'group.id': 'your_consumer_group',
    'auto.offset.reset': 'earliest'  # You can set this to 'latest' or 'none' to control the starting offset.
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

# Set the topic to consume data from
topic = 'your_kafka_topic'

# Initialize the checkpoint (e.g., from a database or configuration)
last_checkpoint_offset = get_last_checkpoint_offset()  # Your implementation

# Subscribe to the Kafka topic
consumer.subscribe([topic])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print("Reached end of partition")
        else:
            print("Error: {}".format(msg.error()))
    else:
        offset = msg.offset()
        if offset > last_checkpoint_offset:
            # Process the new or incremental data
            process_data(msg.value())

            # Update the checkpoint
            last_checkpoint_offset = offset
            update_checkpoint(last_checkpoint_offset)  # Your implementation


# COMMAND ----------

# MAGIC %md
# MAGIC In PySpark, you can perform various types of joins to combine data from two or more DataFrames. The most common types of joins are:
# MAGIC
# MAGIC Inner Join (join or inner)
# MAGIC
# MAGIC An inner join returns only the rows for which there is a match in both DataFrames. Rows that don't have a matching key in both DataFrames are excluded.
# MAGIC Example: df1.join(df2, "key_column", "inner")
# MAGIC Left Join (left or left_outer)
# MAGIC
# MAGIC A left join returns all the rows from the left DataFrame and the matching rows from the right DataFrame. If there is no match in the right DataFrame, null values are included.
# MAGIC Example: df1.join(df2, "key_column", "left")
# MAGIC Right Join (right or right_outer)
# MAGIC
# MAGIC A right join returns all the rows from the right DataFrame and the matching rows from the left DataFrame. If there is no match in the left DataFrame, null values are included.
# MAGIC Example: df1.join(df2, "key_column", "right")
# MAGIC Full Outer Join (outer or full)
# MAGIC
# MAGIC A full outer join returns all the rows when there is a match in either the left or right DataFrame. If there is no match in one of the DataFrames, null values are included.
# MAGIC Example: df1.join(df2, "key_column", "outer")
# MAGIC Left Semi Join (left_semi)
# MAGIC
# MAGIC A left semi join returns all the rows from the left DataFrame for which there is a match in the right DataFrame. It includes only the columns from the left DataFrame.
# MAGIC Example: df1.join(df2, "key_column", "left_semi")
# MAGIC Left Anti Join (left_anti)
# MAGIC
# MAGIC A left anti join returns all the rows from the left DataFrame for which there is no match in the right DataFrame. It includes only the columns from the left DataFrame.
# MAGIC Example: df1.join(df2, "key_column", "left_anti")
# MAGIC Cross Join (cross)
# MAGIC
# MAGIC A cross join returns the Cartesian product of rows from both DataFrames, resulting in a large combined DataFrame. It does not require a specific key column.
# MAGIC Example: df1.crossJoin(df2)
# MAGIC Self Join
# MAGIC
# MAGIC A self-join is used when you want to join a DataFrame with itself, typically using different aliases to distinguish between the two instances of the same DataFrame.
# MAGIC Here's an example of how to use a join in PySpark:

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# Join two DataFrames (df1 and df2) using an inner join on a common key column
result = df1.join(df2, "key_column", "inner")

result.show()


# COMMAND ----------

To use python functions in SQL statement you need to register it

# COMMAND ----------

spark.udf.register("multiple_cols",multiply_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC # PYTHON UDFs

# COMMAND ----------

from pyspark.sql.functions import udf,length
from pyspark.sql.types import *

# COMMAND ----------

def count_chars(col):
    return length(col)

# COMMAND ----------

def count_chars_python(col):
    return len(col)

# COMMAND ----------

df = spark.read.format('delta').load('dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_delta')

# COMMAND ----------

df.withColumn('Country_name_length',count_chars(df.NAME)).display()

# COMMAND ----------

df.withColumn('Country_name_length',count_chars_python(df.NAME)).display()

# COMMAND ----------

from pyspark.sql.functions import udf

# COMMAND ----------

count_chars_python = udf(count_chars_python)

# COMMAND ----------

df.withColumn('Country_name_length',count_chars_python(df.NAME)).display()

# COMMAND ----------


