# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

clientID = dbutils.secrets.get(scope='eventscope',key='eventsclientid')

# COMMAND ----------

tenantID = dbutils.secrets.get(scope='eventscope',key='eventstenantid')

# COMMAND ----------

secret = dbutils.secrets.get(scope='eventscope',key='eventssecret')

# COMMAND ----------

# Set your storage account and container information
spark.conf.set("fs.azure.account.auth.type.azureeventhubsstorage1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azureeventhubsstorage1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azureeventhubsstorage1.dfs.core.windows.net", f"{clientID}")
spark.conf.set("fs.azure.account.oauth2.client.secret.azureeventhubsstorage1.dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azureeventhubsstorage1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantID}/oauth2/token")


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{clientID}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope='eventscope',key='eventssecret'),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(source=f"abfss://eventhubs1@azureeventhubsstorage1.dfs.core.windows.net/",
                 mount_point="/mnt/streamevents",
                 extra_configs = configs )

# COMMAND ----------

display(dbutils.fs.ls('/mnt/streamevents/'))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType

# COMMAND ----------

event_schema = StructType(fields=[
    StructField("EntryTime", StringType(), True),
    StructField("CarModel", StringType(), True),
    StructField("State", StringType(), True),
    StructField("TollAmount", StringType(), True),
    StructField("Tag", StringType(), True),
    StructField("TollId", StringType(), True),
    StructField("LicensePlate", StringType(), True),
    StructField("EventProcessedUtcTime", StringType(), True),
    StructField("PartitionId", StringType(), True),
    StructField("EventEnqueuedUtcTime", StringType(), True),
    StructField("EventMinute", StringType(), True),
    StructField("EventSecond", StringType(), True),
    # Add more fields as needed
])

# COMMAND ----------

car_schema = StructType(fields=[ 
                        StructField("Make",StringType(),True), \
                        StructField("Model",StringType(),True), \
                        StructField("VehicleType",StringType(),True), \
                        StructField("VehicleWeight",StringType(),True),      

])

# COMMAND ----------

df = spark.readStream.json('dbfs:/mnt/streamevents/*/',schema=event_schema)

# COMMAND ----------

from pyspark.sql.functions import from_json

# COMMAND ----------

df_car_info = df.withColumn("parsedData", from_json(df["CarModel"], car_schema))

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

df3 = df_car_info[df_car_info.LicensePlate,df_car_info.parsedData.Make.alias('Make'),df_car_info.parsedData.Model.alias('Model')]

# COMMAND ----------

df3.display()

# COMMAND ----------

import dlt

# COMMAND ----------


