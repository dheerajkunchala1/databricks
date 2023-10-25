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

