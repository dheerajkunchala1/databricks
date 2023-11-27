# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()

# COMMAND ----------

sample_data = [{"name": "John    D.", "age": 30},
  {"name": "Alice   G.", "age": 25},
  {"name": "Bob  T.", "age": 35},
  {"name": "Eve   A.", "age": 28}]

df = spark.createDataFrame(sample_data)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Remove additional spaces in name
def remove_extra_spaces(df, column_name):
    # Remove extra spaces from the specified column
    df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))

    return df_transformed

transformed_df = remove_extra_spaces(df, "name")

transformed_df.show()

# COMMAND ----------

import pyspark.testing
from pyspark.testing.utils import assertDataFrameEqual

# Example 1
df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
assertDataFrameEqual(df1, df2)  # pass, DataFrames are identical

# COMMAND ----------

assertDataFrameEqual(df1, df2)  # pass, DataFrames are identical

# COMMAND ----------

df1 = spark.createDataFrame(data=[("1", 0.1), ("2", 3.23)], schema=["id", "amount"])
df2 = spark.createDataFrame(data=[("1", 0.109), ("2", 3.24)], schema=["id", "amount"])
assertDataFrameEqual(df1, df2, rtol=1e-1)

# COMMAND ----------

import unittest

class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()


    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

# COMMAND ----------

from pyspark.testing.utils import assertDataFrameEqual

class TestTranformation(PySparkTestCase):
    def test_single_space(self):
        sample_data = [{"name": "John    D.", "age": 40},
                       {"name": "Alice   G.", "age": 25},
                       {"name": "Bob  T.", "age": 35},
                       {"name": "Eve   A.", "age": 28}]

        # Create a Spark DataFrame
        original_df = spark.createDataFrame(sample_data)

        # Apply the transformation function from before
        transformed_df = remove_extra_spaces(original_df, "name")

        expected_data = [{"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35},
        {"name": "Eve A.", "age": 48}]

        expected_df = spark.createDataFrame(expected_data)

        assertDataFrameEqual(transformed_df, expected_df)


# COMMAND ----------

unittest.main(argv=[''], verbosity=0, exit=False)

# COMMAND ----------


