# Databricks notebook source
def filter_country(df,country):
    return df.filter(df["country"] == country)

# COMMAND ----------

def population(df,country):
    return df.filter(df["country"] == country)['country','population'].collect()

# COMMAND ----------

def population2(df,countries):
    return df.filter(df["country"].isin(countries))['country','population']

# COMMAND ----------


