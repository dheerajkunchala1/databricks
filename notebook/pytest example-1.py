# Databricks notebook source
from pyspark.sql import functions as F

def calculate_sum(df, column_name):
    return df.agg(F.sum(column_name)).collect()[0][0]


# COMMAND ----------

from pyspark.sql import SparkSession
import pytest
#from my_module import calculate_sum

@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder.master("local").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

def test_calculate_sum(spark_session):
    data = [(1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)]
    schema = ['id', 'name', 'amount']
    test_df = spark_session.createDataFrame(data, schema)

    assert calculate_sum(test_df, 'amount') == 600


# COMMAND ----------

pytest.main()

# COMMAND ----------

Equality Assertions:
assert x == y: Asserts if x equals y.
assert x != y: Asserts if x does not equal y.
assert x in y: Asserts if x is in y.
assert x not in y: Asserts if x is not in y.
assert x is y: Asserts if x is y (checks identity).
Comparison Assertions:
assert x > y: Asserts if x is greater than y.
assert x >= y: Asserts if x is greater than or equal to y.
assert x < y: Asserts if x is less than y.
assert x <= y: Asserts if x is less than or equal to y.
Truth Value Assertions:
assert x: Asserts that x is True.
assert not x: Asserts that x is False.
Exception Assertions:
with pytest.raises(Exception):: Asserts that a specific exception is raised in a block of code.
Example: with pytest.raises(ValueError):
String Assertions:
assert "substring" in string: Asserts if a substring is present in a string.
assert string.startswith("prefix"): Asserts if a string starts with a specific prefix.
assert string.endswith("suffix"): Asserts if a string ends with a specific suffix.
Approximation Assertions:
assert round(x, 2) == 3.14: Asserts if the value of x is approximately 3.14 up to 2 decimal places.
assert pytest.approx(x) == 3.14: Asserts approximate equality for floating-point numbers.
