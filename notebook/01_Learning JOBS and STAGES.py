# Databricks notebook source
df_drivers = spark.sql('select * from formula1_dev.bronze.drivers')

# COMMAND ----------

df_races = spark.sql('select * from formula1_dev.bronze.results')

# COMMAND ----------

df_drivers.rdd.getNumPartitions()

# COMMAND ----------

df_races.rdd.getNumPartitions()

# COMMAND ----------

df_drivers.rdd.getNumPartitions()

# COMMAND ----------

df_drivers.show()

# COMMAND ----------

df_races.show()

# COMMAND ----------

# Function to calculate partition size
import sys

def get_partition_size(iterator):
    yield sys.getsizeof(list(iterator))

# Calculate partition sizes using foreachPartition
partition_sizes = df_races.rdd.foreachPartition(get_partition_size).collect()


# COMMAND ----------


# Display partition sizes
for partition_index, size in enumerate(partition_sizes):
    print(f"Partition {partition_index}: {size} bytes")

# COMMAND ----------

df_races.explain()

# COMMAND ----------

df_joined = df_races.join(df_drivers,on='driverId',how='inner')

# COMMAND ----------

df_joined.explain()

# COMMAND ----------

df_joined.show()

# COMMAND ----------

df_joined2 = df_drivers.join(df_races,on='driverId',how='inner')

# COMMAND ----------

df_joined2.show()

# COMMAND ----------

df_joined2.rdd.getNumPartitions()

# COMMAND ----------

df_joined2.explain()

# COMMAND ----------


