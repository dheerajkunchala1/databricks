# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

clientID = dbutils.secrets.get(scope='kv1',key='clientid')

# COMMAND ----------

tenantID = dbutils.secrets.get(scope='kv1',key='tenantid')

# COMMAND ----------

secret = dbutils.secrets.get(scope='kv1',key='secret')

# COMMAND ----------

#
spark.conf.set("fs.azure.account.auth.type.vehicletollbooth.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.vehicletollbooth.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.vehicletollbooth.dfs.core.windows.net", f"{clientID}")
spark.conf.set("fs.azure.account.oauth2.client.secret.vehicletollbooth.dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.vehicletollbooth.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantID}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{clientID}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope='kv1',key='secret'),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://tolldata@vehicletollbooth.dfs.core.windows.net/",
  mount_point = "/mnt/vehicleraw/tolldata",
  extra_configs = configs)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.silvervehicle.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.silvervehicle.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.silvervehicle.dfs.core.windows.net", f"{clientID}")
spark.conf.set("fs.azure.account.oauth2.client.secret.silvervehicle.dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.silvervehicle.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantID}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{clientID}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope='kv1',key='secret'),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://silverdata@silvervehicle.dfs.core.windows.net/",
  mount_point = "/mnt/silverdata/silverdata",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

pip install dlt

# COMMAND ----------

import dlt

# COMMAND ----------

dbutils.fs.ls('/mnt/vehicleraw/tolldata')

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,ArrayType

# COMMAND ----------

vehicle_schema = StructType(fields=[StructField('Make',StringType(),False), \
                                StructField('Model',StringType(),False), \
                                StructField('VehicleType',StringType(),False), \
                                StructField('VehicleWeight',StringType(),False), \
       ])

# COMMAND ----------

toll_schema = StructType(fields=[StructField('EntryTime',StringType(),False), \
                                StructField('CarModel',StructType(vehicle_schema),False), \
                                StructField('State',StringType(),False), \
                                StructField('TollAmount',StringType(),False), \
                                StructField('Tag',StringType(),False), \
                                StructField('TollId',StringType(),False), \
                                StructField('LicensePlate',StringType(),False), \
                                StructField('PartitionId',StringType(),False), \
                                StructField('EventEnqueuedUtcTime',StringType(),False), \
                                StructField('datetime',StringType(),False) \
       ])


# COMMAND ----------

df= spark.read.format('json').schema(toll_schema).load('/mnt/vehicleraw/tolldata/day=*/minute=*/second=*/*.json')

# COMMAND ----------

df.display()

# COMMAND ----------

vehicle_df = df.select(df.CarModel.Make.alias('Make'),df.CarModel.Model.alias('Model'),df.State.alias('State'),df.datetime.alias('Captured_time'))

# COMMAND ----------

vehicle_df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum,avg,rank,dense_rank,nth_value,first_value,last_value

# COMMAND ----------

vehicle_df.groupBy('Make','Model').count().display()

# COMMAND ----------

w_spec = Window.partitionBy('Make')

# COMMAND ----------

vehicle_df.withColumn('Count',count('Captured_time').over (w_spec)).select('Make','Model','State','Count').distinct().display()

# COMMAND ----------

from pyspark.sql.functions import desc,asc

# COMMAND ----------

w_spec = Window.partitionBy('Make','Model').orderBy(desc('Captured_time')).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# COMMAND ----------



# COMMAND ----------

vehicle_df.withColumn('Count',nth_value('Model',2).over (w_spec)).select('Make','Model','State','Count').distinct().display()

# COMMAND ----------

vehicle_df.write.partitionBy('State').format('delta').mode('append').save('/mnt/silverdata/silverdata/')

# COMMAND ----------


