# Databricks notebook source
raw_folder_path = '/mnt/formula1dl/rawincremental'
silver_folder_path = '/mnt/formula1dl/silverincremental'
gold_folder_path = '/mnt/formula1dl/gold'

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType

# COMMAND ----------

from pyspark.sql.functions import col,lit,current_timestamp

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

# MAGIC %md
# MAGIC ## TESTING AZURE DEVOPS

# COMMAND ----------


