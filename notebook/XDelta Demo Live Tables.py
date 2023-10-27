# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Tables

# COMMAND ----------

# ORDERS TABLES

orders_path = "/mnt/streaming/full_dataset/orders_full.csv"

orders_schema = StructType(fields=[StructField('ORDER_ID',IntegerType(),False), \
                                    StructField('ORDER_DATETIME',StringType(),False), \
                                    StructField('CUSTOMER_ID',IntegerType(),False), \
                                    StructField('ORDER_STATUS',StringType(),False), \
                                    StructField('STORE_ID',IntegerType(),False), \
    ])

# COMMAND ----------

@dlt.table
def orders_bronze():
    df = spark.read.options(schema=orders_schema).csv(orders_path,header=True)
    return df

# COMMAND ----------

# ORDERS_ITEMS

orders_items_path = "/mnt/streaming/full_dataset/order_items_full.csv"

orders_items_schema = StructType(fields=[StructField('ORDER_ID',IntegerType(),False), \
                                    StructField('LINE_ITEM_ID',IntegerType(),False), \
                                    StructField('PRODUCT_ID',IntegerType(),False), \
                                    StructField('UNIT_PRICE',DoubleType(),False), \
                                    StructField('QUANTITY',IntegerType(),False), \
    ])

# COMMAND ----------

@dlt.table
def orders_items_bronze():
    df = spark.read.options(schema=orders_items_schema).csv(orders_items_path,header=True)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER TABLES

# COMMAND ----------

@dlt.table
def orders_silver():
    return (
            dlt.read("orders_bronze")
            .select(
                    'ORDER_ID', \
                    to_date('ORDER_DATETIME',"dd-MMM-yy kk:ss.SS").alias('ORDER_DATE'), \
                    'CUSTOMER_ID', \
                    'ORDER_STATUS', \
                    'STORE_ID',
                    current_timestamp().alias("MODIFIED_DATE")
            )
    )

# COMMAND ----------

@dlt.table
def order_items_silver():
    return(
        dlt.read("orders_items_bronze")
            .select(
                'ORDER_ID', \
                'PRODUCT_ID', \
                'UNIT_PRICE', \
                'QUANTITY', \
                current_timestamp().alias("MODIFIED_DATE")
            )
    )

# COMMAND ----------


