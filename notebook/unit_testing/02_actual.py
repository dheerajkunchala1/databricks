# Databricks notebook source
# MAGIC %run ./01_functions

# COMMAND ----------

df = spark.sql('select * from covid_ecdc.pop_by_age.pop_by_age_aggregated_2019')

# COMMAND ----------

df.show()

# COMMAND ----------

df.filter(df["country"] == "Netherlands").show()

# COMMAND ----------

filter_country(df,"Netherlands").show()

# COMMAND ----------

filter_country(df,"Montenegro").show()

# COMMAND ----------

population(df,"Montenegro")

# COMMAND ----------

int(population(df,"Montenegro")[0][1])

# COMMAND ----------

population2(df,['Montenegro','Netherlands']).collect()

# COMMAND ----------


