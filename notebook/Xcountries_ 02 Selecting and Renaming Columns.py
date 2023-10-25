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


