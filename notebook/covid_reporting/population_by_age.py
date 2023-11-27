# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG covid_ecdc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION covidreporting_bronze
# MAGIC URL 'abfss://raw@covidreportingdl911.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL dbaccessconnector911)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION covidreporting_silver
# MAGIC URL 'abfss://processed@covidreportingdl911.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL dbaccessconnector911)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION covidreporting_lookup
# MAGIC URL 'abfss://ecdc@covidreportingdl911.dfs.core.windows.net/lookupdata'
# MAGIC WITH (STORAGE CREDENTIAL dbaccessconnector911)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS pop_by_age;
# MAGIC CREATE SCHEMA pop_by_age;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE pop_by_age.country_lookup
# MAGIC (
# MAGIC   country STRING,
# MAGIC   country_code_2_digit STRING,
# MAGIC   country_code_3_digit STRING,
# MAGIC   continent STRING,
# MAGIC   population STRING
# MAGIC )
# MAGIC using CSV
# MAGIC OPTIONS (path 'abfss://ecdc@covidreportingdl911.dfs.core.windows.net/lookupdata/country_lookup.csv')

# COMMAND ----------

df = spark.read.format('csv').options(header=True,inferSchema = "True",delimiter='\t').load('abfss://raw@covidreportingdl911.dfs.core.windows.net/population/population_by_age.tsv')

# COMMAND ----------

def drop_columns(datafr,column_names):
    return datafr.select(column_names)

# COMMAND ----------

df_new = drop_columns(df,['2019 ',df[0]])

# COMMAND ----------

df_new.show()

# COMMAND ----------

from pyspark.sql.functions import split

# COMMAND ----------

df_age_group_split = df_new.withColumn('age_group_actual',split(df_new[1],',')[0]).withColumn('age_group',split('age_group_actual','PC_Y')[1]) \
      .withColumn('country_code_2_digit',split(df_new[1],',')[1])  \
      .withColumnRenamed('2019 ','2019') \
       .select ('age_group','country_code_2_digit','2019')

# COMMAND ----------

from pyspark.sql.functions import length,col

# COMMAND ----------

df_country_code_accurate = df_age_group_split.filter(length(col('country_code_2_digit')) == 2)

# COMMAND ----------

df_country_code_accurate

# COMMAND ----------

df_lookup_country = spark.sql('SELECT * FROM covid_ecdc.pop_by_age.country_lookup')

# COMMAND ----------

combined_df = df_country_code_accurate.alias('popbyage').join(df_lookup_country.alias('countrylkp'),'country_code_2_digit','inner').select('country','country_code_2_digit','country_code_3_digit','population','age_group','2019')

# COMMAND ----------

from pyspark.sql.functions import sum

# COMMAND ----------

transformed_df = combined_df.groupBy('country','country_code_2_digit','country_code_3_digit','population').pivot('age_group').agg(sum('2019'))

# COMMAND ----------

transformed_df.write.mode("overwrite").saveAsTable('covid_ecdc.pop_by_age.pop_by_age_aggregated_2019')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from covid_ecdc.pop_by_age.pop_by_age_aggregated_2019

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY covid_ecdc.pop_by_age.pop_by_age_aggregated_2019

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from covid_ecdc.pop_by_age.pop_by_age_aggregated_2019
# MAGIC VERSION AS OF 0

# COMMAND ----------


