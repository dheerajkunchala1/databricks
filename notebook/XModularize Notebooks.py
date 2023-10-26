# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.notebook.run('XModularize_Worker',60,{'input_widget':'From Master Notebook'})

# COMMAND ----------


