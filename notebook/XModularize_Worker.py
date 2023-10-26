# Databricks notebook source
dbutils.widgets.text('input_widget','','provide an input')

# COMMAND ----------

dbutils.widgets.get('input_widget')

# COMMAND ----------


