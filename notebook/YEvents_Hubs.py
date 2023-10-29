# Databricks notebook source
import dlt
import pyspark.sql.types as T
from pyspark.sql.functions import *

# Event Hubs configuration
#EH_NAMESPACE                    = spark.conf.get("iot.ingestion.eh.namespace")
#EH_NAME                         = spark.conf.get("iot.ingestion.eh.name")

#EH_CONN_SHARED_ACCESS_KEY_NAME  = spark.conf.get("iot.ingestion.eh.accessKeyName")
#SECRET_SCOPE                    = spark.conf.get("io.ingestion.eh.secretsScopeName")
#EH_CONN_SHARED_ACCESS_KEY_VALUE = dbutils.secrets.get(scope = SECRET_SCOPE, key = EH_CONN_SHARED_ACCESS_KEY_NAME)

EH_CONN_STR                     = f"Endpoint=sb://dheerajtestevents.servicebus.windows.net/;SharedAccessKeyName=connection_str;SharedAccessKey=yvjpkBQ2GHLPtNs5V5WNCv4pprSTocqDz+AEhLgUGWQ="
# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"dheerajtestevents.servicebus.windows.net:9093",
  "subscribe"                : "testevents2",
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
 # "kafka.request.timeout.ms" : spark.conf.get("iot.ingestion.kafka.requestTimeout"),
 # "kafka.session.timeout.ms" : spark.conf.get("iot.ingestion.kafka.sessionTimeout"),
 # "maxOffsetsPerTrigger"     : spark.conf.get("iot.ingestion.spark.maxOffsetsPerTrigger"),
 # "failOnDataLoss"           : spark.conf.get("iot.ingestion.spark.failOnDataLoss"),
 # "startingOffsets"          : spark.conf.get("iot.ingestion.spark.startingOffsets")
}

# PAYLOAD SCHEMA
payload_ddl = """EntryTime STRING, CarModel STRING, State STRING, TollAmount STRING, Tag STRING, TollId STRING, LicensePlate STRING, EventProcessedUtcTime STRING, PartitionId STRING, EventEnqueuedUtcTime STRING"""
payload_schema = T._parse_datatype_string(payload_ddl)

# Basic record parsing and adding ETL audit columns

 #df=  spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load().transform(parse)

df = spark.read.format("kafka").options(**KAFKA_OPTIONS).load()
  

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType

# Define your Azure Service Bus connection string
service_bus_connection_string = "Endpoint=sb://sbdheeraj.servicebus.windows.net/;SharedAccessKeyName=test_topic_SAS;SharedAccessKey=DKCeo9EjPYHR0opCh2lNTqzq1l1sAbdNI+ASbG1gT7M=;EntityPath=test_topic"

# Define the Azure Service Bus parameters
service_bus_params = {
    "eventhubs.connectionString": service_bus_connection_string,
    "eventhubs.consumerGroup": "$Default",  # Change this to your consumer group
}

# Define the schema for your data
schema = StructType([
    StructField("EntryTime", StringType(), True),
    StructField("CarModel", StringType(), True),
    StructField("State", StringType(), True),
    StructField("TollAmount", StringType(), True),
    StructField("Tag", StringType(), True),
    StructField("TollId", StringType(), True),
    StructField("LicensePlate", StringType(), True),
    StructField("EventProcessedUtcTime", StringType(), True),
    StructField("PartitionId", StringType(), True),
    StructField("EventEnqueuedUtcTime", StringType(), True)
    # Add more fields as needed
])

# Create a Spark session
spark = SparkSession.builder.appName("ServiceBusStreaming").getOrCreate()

# Read data from Azure Service Bus using Structured Streaming
service_bus_stream = spark.readStream \
    .format("eventhubs") \
    .options(**service_bus_params) \
    .load()

# Deserialize the data using your schema
deserialized_stream = service_bus_stream.selectExpr("cast(body as string) as message") \
    .select(from_json("message", schema).alias("data"))

# Perform processing or analysis on the data
# For example, you can write the data to another location or perform real-time analytics.

# Start the streaming query
query = deserialized_stream.writeStream \
    .outputMode("append") \
    .format("console")  # You can change the output mode and format

query.start().awaitTermination()


# COMMAND ----------

service_bus_stream.display()

# COMMAND ----------

import org.apache.spark.eventhubs.ConnectionStringBuilder

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType

# Define your Azure Service Bus connection string
service_bus_connection_string = "Endpoint=sb://sbdheeraj.servicebus.windows.net/;SharedAccessKeyName=test_topic_SAS;SharedAccessKey=DKCeo9EjPYHR0opCh2lNTqzq1l1sAbdNI+ASbG1gT7M=;EntityPath=test_topic"

# Define the Azure Service Bus parameters
service_bus_params = {
    "eventhubs.connectionString": service_bus_connection_string,
    "eventhubs.consumerGroup": "$Default",  # Change this to your consumer group
}

# Define the schema for your data
schema = StructType([
    StructField("EntryTime", StringType(), True),
    StructField("CarModel", StringType(), True),
    StructField("State", StringType(), True),
    StructField("TollAmount", StringType(), True),
    StructField("Tag", StringType(), True),
    StructField("TollId", StringType(), True),  # Removed duplicated field
    StructField("EventProcessedUtcTime", StringType(), True),
    StructField("PartitionId", StringType(), True),
    StructField("EventEnqueuedUtcTime", StringType(), True)
    # Add more fields as needed
])

# Create a Spark session
spark = SparkSession.builder.appName("ServiceBusStreaming").getOrCreate()

# Read data from Azure Service Bus using Structured Streaming
service_bus_stream = spark.read \
    .format("eventhubs") \
    .options(**service_bus_params) \
    .load()

# Deserialize the data using your schema
# deserialized_stream = service_bus_stream.selectExpr("cast(body as string) as message") #\
   # .select(from_json("message", schema).alias("data"))

# Perform processing or analysis on the data
# For example, you can write the data to another location or perform real-time analytics.

# Start the streaming query
#query = deserialized_stream.writeStream \
#    .outputMode("append") \
#    .format("console")  # You can change the output mode and format

# query.start().awaitTermination()


# COMMAND ----------

service_bus_stream.display()

# COMMAND ----------

df = (spark.read
  .format("kafka")
  .option("kafka.bootstrap.servers", "Endpoint=sb://sbdheeraj.servicebus.windows.net/;SharedAccessKeyName=test_topic_SAS;SharedAccessKey=DKCeo9EjPYHR0opCh2lNTqzq1l1sAbdNI+ASbG1gT7M=")
  .option("subscribe", "test_topic")
  .load()
)

# COMMAND ----------

df.display()

# COMMAND ----------

import org.apache.spark.eventhubs

# COMMAND ----------

import dlt
import pyspark.sql.types as T
from pyspark.sql.functions import *

# Event Hubs configuration
EH_NAMESPACE                    = spark.conf.get("iot.ingestion.eh.namespace")
EH_NAME                         = spark.conf.get("iot.ingestion.eh.name")

EH_CONN_SHARED_ACCESS_KEY_NAME  = spark.conf.get("iot.ingestion.eh.accessKeyName")
SECRET_SCOPE                    = spark.conf.get("io.ingestion.eh.secretsScopeName")
EH_CONN_SHARED_ACCESS_KEY_VALUE = dbutils.secrets.get(scope = SECRET_SCOPE, key = EH_CONN_SHARED_ACCESS_KEY_NAME)

EH_CONN_STR                     = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"
# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : spark.conf.get("iot.ingestion.kafka.requestTimeout"),
  "kafka.session.timeout.ms" : spark.conf.get("iot.ingestion.kafka.sessionTimeout"),
  "maxOffsetsPerTrigger"     : spark.conf.get("iot.ingestion.spark.maxOffsetsPerTrigger"),
  "failOnDataLoss"           : spark.conf.get("iot.ingestion.spark.failOnDataLoss"),
  "startingOffsets"          : spark.conf.get("iot.ingestion.spark.startingOffsets")
}

# PAYLOAD SCHEMA
payload_ddl = """battery_level BIGINT, c02_level BIGINT, cca2 STRING, cca3 STRING, cn STRING, device_id BIGINT, device_name STRING, humidity BIGINT, ip STRING, latitude DOUBLE, lcd STRING, longitude DOUBLE, scale STRING, temp  BIGINT, timestamp BIGINT"""
payload_schema = T._parse_datatype_string(payload_ddl)

# Basic record parsing and adding ETL audit columns
def parse(df):
  return (df
    .withColumn("records", col("value").cast("string"))
    .withColumn("parsed_records", from_json(col("records"), payload_schema))
    .withColumn("iot_event_timestamp", expr("cast(from_unixtime(parsed_records.timestamp / 1000) as timestamp)"))
    .withColumn("eh_enqueued_timestamp", expr("timestamp"))
    .withColumn("eh_enqueued_date", expr("to_date(timestamp)"))
    .withColumn("etl_processed_timestamp", col("current_timestamp"))
    .withColumn("etl_rec_uuid", expr("uuid()"))
    .drop("records", "value", "key")
  )

@dlt.create_table(
  comment="Raw IOT Events",
  table_properties={
    "quality": "bronze",
    "pipelines.reset.allowed": "false" # preserves the data in the delta table if you do full refresh
  },
  partition_cols=["eh_enqueued_date"]
)
@dlt.expect("valid_topic", "topic IS NOT NULL")
@dlt.expect("valid records", "parsed_records IS NOT NULL")
def iot_raw():
  return (
   spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
    .transform(parse)
  )
