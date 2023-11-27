# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_data(input_df):
    # Example transformation: Adding a new column 'new_column' based on existing columns
    output_df = input_df.withColumn("new_column", col("existing_column") * 2)
    return output_df


# COMMAND ----------


