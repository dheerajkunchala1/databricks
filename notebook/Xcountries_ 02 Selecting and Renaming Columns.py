# Databricks notebook source
path = "/FileStore/tables/countries"

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

countries_df = spark.read.format('csv').options(header=True,schema=countries_schema).load('dbfs:/FileStore/tables/countries/countries.csv')

# COMMAND ----------

display(countries_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Select Columns

# COMMAND ----------

countries_df.select('NAME','CAPITAL','POPULATION').display()

# COMMAND ----------

countries_df.select(countries_df.NAME,countries_df.CAPITAL,countries_df.POPULATION).display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

countries_df.select(countries_df.NAME,countries_df.CAPITAL,countries_df.POPULATION,current_timestamp().alias('ingestion_date')).withColumnRenamed('NAME','country_name').display()

# COMMAND ----------

regions_df = spark.read.format('csv').options(header=True,infer_schema=True).load('dbfs:/FileStore/tables/country_regions')

# COMMAND ----------

regions_schema = StructType(fields=[StructField('ID',IntegerType(),False), \
                                    StructField('NAME',StringType(),False)])

# COMMAND ----------

regions_df = spark.read.format('csv').options(header=True,schema=regions_schema).load('dbfs:/FileStore/tables/country_regions')

# COMMAND ----------

regions_df.display()

# COMMAND ----------

regions_df.withColumn('ingestion_date',current_timestamp()).display()

# COMMAND ----------

countries_df = countries_df.withColumn('pop_in_millions',countries_df.POPULATION/100)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_pop = countries_df.select(countries_df.pop_in_millions.cast(IntegerType()))

# COMMAND ----------

countries_pop.display()

# COMMAND ----------

from pyspark.sql.functions import round

# COMMAND ----------

countries_pop = countries_df.withColumn('pop_millions',round(countries_df.POPULATION/100000,3))

# COMMAND ----------

countries_pop.dtypes

# COMMAND ----------

countries_pop.select(round(countries_pop['pop_millions'],3)).display()

# COMMAND ----------

from pyspark.sql.functions import asc,desc

# COMMAND ----------

countries_pop.sort(countries_pop.pop_in_millions.asc()).display()

# COMMAND ----------

from pyspark.sql.functions import concat_ws,lower,split

# COMMAND ----------

test_df=countries_df.select(concat_ws('-',lower(countries_df.COUNTRY_CODE),countries_df.NAME).alias('concated'))

# COMMAND ----------

test_df.select(split(test_df.concated,'-')[1]).display()

# COMMAND ----------

countries_df.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,current_timezone,current_date,month,day,dayofmonth,weekofyear,year,to_date

# COMMAND ----------

countries_df.withColumn('Ingested_Year',year(current_date())).display()

# COMMAND ----------

countries_df.withColumn('date_formatted',to_date(lit('27/10/1985'),'DD/MM/YYYY')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Filtering Dataframe/Where Clause

# COMMAND ----------

countries_df.filter(countries_df.pop_in_millions > 100).display()

# COMMAND ----------

from pyspark.sql.functions import locate

# COMMAND ----------

countries_df.filter(locate('bu',countries_df.CAPITAL)==2).display()

# COMMAND ----------

countries_df.filter((locate('bu',countries_df.CAPITAL)==2) & (countries_df.pop_in_millions > 1000000)  ).display()

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

countries_df.withColumn('name_length',when(countries_df.POPULATION > 10000000,'large').when(countries_df.POPULATION < 10000000,'Not Large').otherwise('unknown')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC EXPR function

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

countries_df.select(expr('NAME as country_name')).display()

# COMMAND ----------

countries_df.withColumn('area_class',expr("case when AREA_KM2 > 100000 then 'large' when AREA_KM2 <= 100000 and AREA_KM2 > 300000 then 'medium' else 'small' end")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC GROUPING FUNCTIONS

# COMMAND ----------

from pyspark.sql.functions import sum,min

# COMMAND ----------

countries_df.groupBy('REGION_ID').sum('pop_in_millions').display()

# COMMAND ----------

countries_df.groupBy('REGION_ID').agg(sum('pop_in_millions'),min('pop_in_millions')).display()

# COMMAND ----------

countries_df.groupBy('REGION_ID','SUB_REGION_ID').agg(sum('pop_in_millions').alias('total'),min('pop_in_millions').alias('min')).display()

# COMMAND ----------

countries_df.groupBy('REGION_ID','SUB_REGION_ID').agg(sum('pop_in_millions').alias('total'),min('pop_in_millions').alias('min')).sort('total').display()

# COMMAND ----------

countries_df.groupBy('SUB_REGION_ID').pivot('REGION_ID').sum('pop_in_millions').display()

# COMMAND ----------

# MAGIC %md
# MAGIC JOINING TABLES

# COMMAND ----------

countries_df.join(regions_df,countries_df.REGION_ID == regions_df.ID,'inner').display()

# COMMAND ----------

countries_df.union(countries_df).display()

# COMMAND ----------

countries_df.groupBy('NAME').pivot('REGION_ID').sum('pop_in_millions').display()

# COMMAND ----------

pivot_countries = countries_df.groupBy('NAME').pivot('REGION_ID').sum('pop_in_millions')

# COMMAND ----------

pivot_countries.select('NAME',expr("stack(5,'10',10,'20',20,'30',30,'40',40,'50',50) as (REGION_ID,pop_in_millions)")).display()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

countries_pd = countries_df.toPandas()

# COMMAND ----------

countries_pd.head()

# COMMAND ----------

countries_pd.iloc(0)

# COMMAND ----------

# MAGIC %md
# MAGIC # DELTA LAKES

# COMMAND ----------

countries_df.write.format('delta').save('/countries_delta')

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/formula1dl/')

# COMMAND ----------

countries_df.write.format('delta').save('/mnt/formula1dl/silver/countries_delta')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE delta_lake_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table delta_lake_db.countries_managed_delta

# COMMAND ----------

countries_df.write.format('delta').saveAsTable('delta_lake_db.countries_managed_delta')

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE  EXTENDED delta_lake_db.countries_managed_delta

# COMMAND ----------

countries_df.write.format('delta').option("path","'/mnt/formula1dl/silver/countries_delta'").mode('overwrite').saveAsTable('delta_lake_db.countries_managed_ext_delta')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta_lake_db.countries_managed_ext_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from delta_lake_db.countries_managed_ext_delta

# COMMAND ----------

from delta.tables import *


# COMMAND ----------

countries_2 = _sqldf

# COMMAND ----------

countries_2 = countries_2.filter("REGION_ID in (10,20,30)")

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

countries_2.NAME = upper(countries_2.NAME)

# COMMAND ----------

countries_2 = countries_2.withColumn('NAME',upper(countries_2.NAME))

# COMMAND ----------

countries_2.display()

# COMMAND ----------

countries_2.write.format('delta').saveAsTable('delta_lake_db.countries_managed_ext_delta_upper')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta_lake_db.countries_managed_ext_delta
# MAGIC USING delta_lake_db.countries_managed_ext_delta_upper
# MAGIC ON delta_lake_db.countries_managed_ext_delta.COUNTRY_ID = delta_lake_db.countries_managed_ext_delta_upper.COUNTRY_ID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     NAME = delta_lake_db.countries_managed_ext_delta_upper.NAME
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from delta_lake_db.countries_managed_ext_delta

# COMMAND ----------



# COMMAND ----------

c_df = spark.read.format('delta').load('/mnt/formula1dl/silver/countries_delta')

# COMMAND ----------

c_df.display()

# COMMAND ----------


