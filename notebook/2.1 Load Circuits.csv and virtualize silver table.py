# Databricks notebook source
# MAGIC %run ./includes

# COMMAND ----------

# MAGIC %run ./common_functions

# COMMAND ----------

dbutils.widgets.text("p_filedate", "2021-03-28")
v_filedate = dbutils.widgets.get("p_filedate")

# COMMAND ----------

dbutils.widgets.text("p_datasource", "ERGAST_API")
v_datasource = dbutils.widgets.get("p_datasource")

# COMMAND ----------

circuits_schema = StructType(fields=[StructField('circuitId',IntegerType(),False), \
                                     StructField('circuitRef',StringType(),True), \
                                     StructField('name',StringType(),True), \
                                     StructField('location',StringType(),True), \
                                     StructField('country',StringType(),True), \
                                     StructField('lat',DoubleType(),True), \
                                     StructField('lng',DoubleType(),True), \
                                     StructField('alt',IntegerType(),True), \
                                     StructField('url',StringType(),True)
    ])

# COMMAND ----------

path = f'{raw_folder_path}/{v_filedate}/circuits.csv'

# COMMAND ----------

if file_exists(path):
    circuits_df = spark.read.option("header","true").schema(circuits_schema).format('csv').load(path)
else:
    dbutils.notebook.exit("File doesn't exist")

# COMMAND ----------

circuits_modified_df = circuits_df.drop('url')  \
                                  .withColumnRenamed('circuitId','circuit_id') \
                                  .withColumnRenamed('circuitRef','circuit_ref') \
                                  .withColumnRenamed('lat','latitude') \
                                  .withColumnRenamed('lng','longitude') \
                                  .withColumnRenamed('alt','altitude') \
                                  .withColumn('ingestion_date',current_timestamp()) \
                                  .withColumn('file_date',lit(v_filedate)) \
                                  .withColumn('data_source',lit(v_datasource)) 

# COMMAND ----------

display(circuits_modified_df)

# COMMAND ----------

circuits_modified_df.write.format('delta').mode('overwrite').saveAsTable('silver.circuits_processed')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.circuits_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from silver.circuits_processed 

# COMMAND ----------

dbutils.notebook.exit("Success")
