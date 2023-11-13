# Databricks notebook source
import unittest

# Function to calculate the average of a list of numbers
def calculate_average(numbers):
    if not numbers:
        raise ValueError("Empty list")
    return sum(numbers) / len(numbers)

# Function to validate if a string is a palindrome
def is_palindrome(s):
    s = s.lower().replace(" ", "")
    return s == s[::-1]

# Define test cases
class TestDataProcessing(unittest.TestCase):
    
    def test_calculate_average(self):
        # Test for a valid list
        self.assertEqual(calculate_average([1, 2, 3, 4, 5]), 3.0)
        
        # Test for an empty list
        with self.assertRaises(ValueError):
            calculate_average([])
        
    def test_is_palindrome(self):
        # Test for a palindrome
        self.assertTrue(is_palindrome("A man a plan a canal Panama"))
        
        # Test for a non-palindrome
        self.assertFalse(is_palindrome("Databricks"))

# Run the tests
unittest.main(argv=[''], exit=False)


# COMMAND ----------

from pyspark.sql import SparkSession

def filter_by_age(df, min_age):
    return df.filter(df['age'] > min_age)


# COMMAND ----------

import unittest
from pyspark.sql import SparkSession
#from my_module import filter_by_age  # Assuming the function is in a file named 'my_module'

class TestFilterByAge(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("unittest").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_filter_by_age(self):
        data = [(1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 20)]
        schema = ['id', 'name', 'age']
        test_df = self.spark.createDataFrame(data, schema)

        result = filter_by_age(test_df, 25)
        expected_result = test_df.filter(test_df['age'] > 25)

        self.assertEqual(result.collect(), expected_result.collect())

if __name__ == '__main__':
    unittest.main()


# COMMAND ----------

data = [(1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 20)]
schema = ['id', 'name', 'age']
test_df = spark.createDataFrame(data, schema)


# COMMAND ----------

result = filter_by_age(test_df, 25)

# COMMAND ----------

result.show()

# COMMAND ----------

expected_result = test_df.filter(test_df['age'] > 25)

# COMMAND ----------



# COMMAND ----------

def reverse(s):
  return s[::-1]

# COMMAND ----------

import unittest

class TestHelpers(unittest.TestCase):
    def test_reverse(self):
        self.assertEqual(reverse('abc'), 'cba')

r = unittest.main(argv=[''], verbosity=2, exit=False)
assert r.result.wasSuccessful(), 'Test failed; see logs above'

# COMMAND ----------

from pyspark.sql.functions import explode

# Assuming 'df' is your DataFrame
df = spark.createDataFrame([(1, {"name": "Alice", "age": 30}), (2, {"name": "Bob", "age": 25})], ["id", "details"])

exploded_df = df.select("id", explode("details").alias("key", "value"))
exploded_df.show()



# COMMAND ----------

df.display()

# COMMAND ----------


