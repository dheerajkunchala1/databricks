# Databricks notebook source
# MAGIC %md
# MAGIC #MOUNT STREAMING DATA ADLS GEN2

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

clientID = dbutils.secrets.get(scope='streaming_data',key='clientid')

# COMMAND ----------

tenantID = dbutils.secrets.get(scope='streaming_data',key='tenantid')

# COMMAND ----------

secret = dbutils.secrets.get(scope='streaming_data',key='secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl9119.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl9119.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl9119.dfs.core.windows.net", f"{clientID}")
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl9119.dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl9119.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantID}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls('abfss://streaming-demo@formula1dl9119.dfs.core.windows.net'))

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{clientID}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope='streaming_data',key='secret'),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://streaming-demo@formula1dl9119.dfs.core.windows.net/",
  mount_point = "/mnt/streaming",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls('/mnt/streaming')

# COMMAND ----------

# MAGIC %md
# MAGIC # STREAMING DATASET SIMULATOR

# COMMAND ----------

orders_full =  spark.read.options(inferSchema=True).csv("/mnt/streaming/full_dataset/orders_full.csv",header=True)

# COMMAND ----------

orders_full.display()

# COMMAND ----------

order_1 = orders_full.filter(orders_full.ORDER_ID == 1)

# COMMAND ----------

order_1.write.format('csv').options(header=True).mode('append').save("/mnt/streaming/streaming_dataset/orders_streaming.csv")

# COMMAND ----------

spark.read.csv("/mnt/streaming/streaming_dataset/orders_streaming.csv").display()

# COMMAND ----------

order_2 = orders_full.filter(orders_full.ORDER_ID == 2)

# COMMAND ----------

order_2.write.format('csv').options(header=True).mode('append').save("/mnt/streaming/streaming_dataset/orders_streaming.csv")

# COMMAND ----------

spark.read.format('csv').options(header=True).load("/mnt/streaming/streaming_dataset/orders_streaming.csv").display()

# COMMAND ----------

order_3 = orders_full.filter(orders_full.ORDER_ID == 3)

# COMMAND ----------

order_3.write.format('csv').options(header=True).mode('append').save("/mnt/streaming/streaming_dataset/orders_streaming.csv")

# COMMAND ----------

order_4 = orders_full.filter(orders_full.ORDER_ID == 4)

# COMMAND ----------

order_4.write.format('csv').options(header=True).mode('append').save("/mnt/streaming/streaming_dataset/orders_streaming.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # READING A DATASTREAM

# COMMAND ----------

from pyspark.sql.types import IntegerType,TimestampType,StringType,StructField,StructType

# COMMAND ----------

orders_schema = StructType(fields=[StructField('ORDER_ID',IntegerType(),False), \
                                    StructField('ORDER_DATETIME',StringType(),False), \
                                    StructField('CUSTOMER_ID',IntegerType(),False), \
                                    StructField('ORDER_STATUS',StringType(),False), \
                                    StructField('STORE_ID',IntegerType(),False), \
    ])

# COMMAND ----------

orders_streaming_path = "/mnt/streaming/streaming_dataset/orders_streaming.csv"

# COMMAND ----------

orders_sdf = spark.readStream.csv(orders_streaming_path,orders_schema,header=True)

# COMMAND ----------

orders_sdf.isStreaming

# COMMAND ----------

orders_sdf.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Writing to a Data Stream

# COMMAND ----------

streamingQuery = orders_sdf.writeStream.format('delta').option('checkpointLocation',"/mnt/streaming/streaming_dataset/orders_stream_sink/checkpointLocation").start("/mnt/streaming/streaming_dataset/orders_stream_sink")

# COMMAND ----------

streamingQuery.isActive

# COMMAND ----------

streamingQuery.lastProgress

# COMMAND ----------

streamingQuery.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC Create database streaming_db

# COMMAND ----------

streamingQuery = orders_sdf.writeStream.format('delta').option('checkpointLocation',"/mnt/streaming/streaming_dataset/streaming_db/managed/_checkpointLocation").toTable("streaming_db.orders_m")

# COMMAND ----------

# MAGIC %md
# MAGIC #Auto Loader

# COMMAND ----------

orders_sdf = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").schema(orders_schema).load(orders_streaming_path,header=True)

# COMMAND ----------

orders_sdf.display()

# COMMAND ----------


