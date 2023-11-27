# Databricks notebook source
from operator import add
rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
sorted(rdd.reduceByKey(add).collect())
[('a', 2), ('b', 1)]

# COMMAND ----------


