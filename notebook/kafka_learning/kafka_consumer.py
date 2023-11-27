# Databricks notebook source
pip install kafka-python

# COMMAND ----------

from kafka import KafkaConsumer

# COMMAND ----------

import json,time

# COMMAND ----------

if __name__ == '__main__':
    consumer = KafkaConsumer('registered_user',bootstrap_servers='3.18.82.133:9092',auto_offset_reset="earliest",group_id = "consumer-group-a")

# COMMAND ----------

print('starting the consumer')

# COMMAND ----------

for msg in consumer:
    print("Registered User = {}".format(json.loads(msg.value)))

# COMMAND ----------

while 1 = 1:
    

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaMessageConsumer") \
    .getOrCreate()

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "3.18.82.133:9092",
    "subscribe": "registered_user"
}

# Define the schema for the incoming JSON messages
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
])

# Read messages from Kafka topic as a DataFrame
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"]) \
    .option("subscribe", kafka_params["subscribe"]) \
    .load()

# Deserialize the message value as JSON
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING) as value")

# Define your processing logic here
# For example, you can select and display the 'value' column
query = kafka_stream \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batch_df, batch_id: batch_df.select("value").show()) \
    .start()

query.awaitTermination()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaMessageConsumer") \
    .getOrCreate()

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "3.18.82.133:9092",
    "subscribe": "registered_user"
}

# Define the schema for the incoming JSON messages
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
])

# Read messages from Kafka topic as a DataFrame
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"]) \
    .option("subscribe", kafka_params["subscribe"]) \
    .load()

# Deserialize the message value as JSON
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING) as value")

# Define your processing logic here
# For example, you can select and display the 'value' column
query = kafka_stream \
    .writeStream \
    .outputMode("append")  \
    .foreachBatch(lambda batch_df, batch_id: batch_df.show()) \
    .start()

query.awaitTermination()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaMessageCounter") \
    .getOrCreate()

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "3.18.82.133:9092",
    "subscribe": "registered_user"
}

# Define the schema for the incoming JSON messages
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
])

# Read messages from Kafka topic as a DataFrame
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"]) \
    .option("subscribe", kafka_params["subscribe"]) \
    .load()

# Deserialize the message value as JSON
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING) as value")

# Perform a count aggregation
message_count = kafka_stream.groupBy().agg(count("*").alias("message_count"))

# Define your processing logic here
query = message_count \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda batch_df, batch_id: batch_df.show()) \
    .start()

query.awaitTermination()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaMessageCounter") \
    .getOrCreate()

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "3.18.82.133:9092",
    "subscribe": "registered_user"
}

# Define the schema for the incoming JSON messages
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
])

# Read messages from Kafka topic as a DataFrame
kafka_stream = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"]) \
    .option("subscribe", kafka_params["subscribe"]) \
    .load()

# Deserialize the message value as JSON
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING) as value")

# Perform a count aggregation
message_count = kafka_stream.groupBy().agg(count("*").alias("message_count"))

display(message_count)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StructField, StringType
import json

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaMessageCounter") \
    .getOrCreate()

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "3.18.82.133:9092",
    "subscribe": "registered_user"
}

# Define the schema for the incoming JSON messages
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
    StructField("Timestamp",StringType(),True),
    StructField("Topic",StringType(),True),
    StructField("Partition",StringType(),True),
    StructField("Offset",StringType(),True)

])

starting_offsets = {
    "registered_user": {
        "0": 1154  # Start from offset 23 in partition 0
    }
}

starting_offsets_json = json.dumps(starting_offsets)

# Read messages from Kafka topic as a DataFrame
kafka_stream = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"]) \
    .option("subscribe", kafka_params["subscribe"]) \
    .option("startingOffsets",starting_offsets_json) \
    .load()

# Deserialize the message value as JSON
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING) as value","CAST(key AS STRING) as key","CAST(Offset AS STRING) as Offset")




# COMMAND ----------

display(kafka_stream)

# COMMAND ----------


