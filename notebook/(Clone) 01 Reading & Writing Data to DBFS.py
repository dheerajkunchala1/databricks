# Databricks notebook source
dbutils.fs.ls("/FileStore/tables/countries")

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r dbfs:/FileStore/tables/countries

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/countries")

# COMMAND ----------

countries_df = spark.read.format("csv").options(header=True,inferSchema=True).load('dbfs:/FileStore/tables/countries/countries.csv')

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

countries_schema = StructType(fields=[StructField('COUNTRY_ID',IntegerType(),False), \
                                    StructField('NAME',StringType(),False), \
                                    StructField('NATIONALITY',StringType(),False), \
                                    StructField('COUNTRY_CODE',StringType(),False), \
                                    StructField('ISO_ALPHA2',StringType(),False), \
                                    StructField('CAPITAL',StringType(),False), \
                                    StructField('POPULATION',IntegerType(),False), \
                                    StructField('AREA_KM2',DoubleType(),False), \
                                    StructField('REGION_ID',IntegerType(),True), \
                                    StructField('SUB_REGION_ID',IntegerType(),True), \
                                    StructField('INTERMEDIATE_REGION_ID',IntegerType(),True), \
                                    StructField('ORGANIZATION_REGION_ID',IntegerType(),True), \
    ])

# COMMAND ----------

countries_df = spark.read.format("csv").options(header=True,schema = countries_schema).load('dbfs:/FileStore/tables/countries/countries.csv')

# COMMAND ----------

display(countries_df.show())

# COMMAND ----------

countries_df_multiline = spark.read.format("json").options(schema = countries_schema,multiLine = True).load('dbfs:/FileStore/tables/countries/countries_multi_line.json')

# COMMAND ----------

display(countries_df_multiline)

# COMMAND ----------

display(countries_df_multiline.filter(countries_df_multiline.NAME=='Antarctica'))

# COMMAND ----------

countries_df.write.partitionBy('REGION_ID').mode('overwrite').parquet('dbfs:/FileStore/tables/countries/countries_out')

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/countries/countries_out/")

# COMMAND ----------

countries_df_parq = spark.read.options(schema = countries_schema).format('parquet').load('dbfs:/FileStore/tables/countries/countries_out/')

# COMMAND ----------

display(countries_df_parq)

# COMMAND ----------


