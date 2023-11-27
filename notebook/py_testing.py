# Databricks notebook source
def reverse(s):
    return s[::-1]

import unittest

class TestHelpers(unittest.TestCase):
    def test_reverse(self):
        self.assertEqual(reverse('abc'), 'cba')

r = unittest.main(argv=[''], verbosity=2, exit=False)
assert r.result.wasSuccessful(), 'Test failed; see logs above'


# COMMAND ----------

def add(x, y):
    return x + y

import unittest

class TestAddFunction(unittest.TestCase):

    def test_add_positive_numbers(self):
        result = add(3, 5)
        expected_result = 8
        self.assertEqual(result, expected_result, f"Test Case 1 failed: {result} != {expected_result}")

    def test_add_negative_numbers(self):
        result = add(-2, -4)
        expected_result = -6
        self.assertEqual(result, expected_result, f"Test Case 2 failed: {result} != {expected_result}")

    def test_add_with_zero(self):
        result = add(0, 7)
        expected_result = 7
        self.assertEqual(result, expected_result, f"Test Case 3 failed: {result} != {expected_result}")

r = unittest.main(argv=[''], verbosity=2, exit=False)
assert r.result.wasSuccessful(), 'Test failed; see logs above'


# COMMAND ----------

# MAGIC %run ./pytest2

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestTransformData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up a SparkSession for the entire test class
        cls.spark = SparkSession.builder.master("local").appName("unittest").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession after all tests in the class have run
        cls.spark.stop()

    def setUp(self):
        # Create a sample DataFrame for each test
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 22)]
        columns = ["name", "existing_column"]
        self.input_df = self.spark.createDataFrame(data, columns)

    def tearDown(self):
        # Clean up resources after each test
        pass

    def test_transform_data(self):
        # Call the transformation function
        result_df = transform_data(self.input_df)

        # Define the expected DataFrame
        expected_data = [("Alice", 25, 50), ("Bob", 30, 60), ("Charlie", 22, 44)]
        expected_columns = ["name", "existing_column", "new_column"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        # Compare the actual and expected DataFrames
        self.assertTrue(result_df.collect() == expected_df.collect())

r = unittest.main(argv=[''], verbosity=2, exit=False)
assert r.result.wasSuccessful(), 'Test failed; see logs above'


# COMMAND ----------


