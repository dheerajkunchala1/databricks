# Databricks notebook source
# MAGIC %run ./01_functions

# COMMAND ----------

df = spark.sql('select * from covid_ecdc.pop_by_age.pop_by_age_aggregated_2019')

# COMMAND ----------

from unittest import *
from pyspark.sql import SparkSession

# COMMAND ----------

class PopulationTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
                                .appName("test-case-pyspark") \
                                .getOrCreate()

    def test_population(self):
        pop_result = int(population(df,"Montenegro")[0][1])

        self.assertEqual(pop_result,622182,"Result for Montenegro should be 622182")

# COMMAND ----------

def suite():
    suite = TestSuite()
    suite.addTest(PopulationTestCase('test_population'))
    return suite

# COMMAND ----------

runner = TextTestRunner()
runner.run(suite())

# COMMAND ----------

list_countries = ['Montenegro','Netherlands']

# COMMAND ----------


class PopulationTestCase2(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
                                .appName("test-case2-pyspark") \
                                .getOrCreate()
        cls.df = spark.sql('select * from covid_ecdc.pop_by_age.pop_by_age_aggregated_2019')
        cls.list_countries = ['Montenegro','Netherlands']

    def test_population2(self):
        pop_result = population2(self.df,self.list_countries).collect()

        pop_dict = dict()
        for row in pop_result:
            pop_dict[row["country"]] = int(row["population"])

        self.assertEqual(pop_dict['Montenegro'],622182,"Result for Montenegro should be 622182")
        self.assertEqual(pop_dict['Netherlands'],17282163,"Result for Montenegro should be 17282163")

# COMMAND ----------

def suite2():
    suite2 = TestSuite()
    suite2.addTest(PopulationTestCase2('test_population2'))
    return suite2

# COMMAND ----------

runner = TextTestRunner()
runner.run(suite2())

# COMMAND ----------


