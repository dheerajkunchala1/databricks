# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, date_sub, lit
from pyspark.sql.types import DateType


# COMMAND ----------

spark = SparkSession.builder.appName("CalendarTable").getOrCreate()


# COMMAND ----------

start_date = "2023-01-01"
end_date = "2023-12-31"


# COMMAND ----------

date_range_df = spark.range(0, end_date - start_date.days + 1, 1).selectExpr("id as date_offset")


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("CalendarTable").getOrCreate()

start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

date_range = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)]
date_range_df = spark.createDataFrame(date_range, DateType()).toDF("date")

# Add additional columns for year, month, day, day of the week, etc.
date_range_df = date_range_df.withColumn("year", col("date").substr(1, 4))
date_range_df = date_range_df.withColumn("month", col("date").substr(6, 2))
date_range_df = date_range_df.withColumn("day", col("date").substr(9, 2))
date_range_df = date_range_df.withColumn("day_of_week", date_format(col("date"), "u"))

date_range_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("CalendarTable").getOrCreate()

# Set the legacy timeParserPolicy
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

date_range = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)]
date_range_df = spark.createDataFrame(date_range, DateType()).toDF("date")

# Add additional columns for year, month, day, and day of the week
date_range_df = date_range_df.withColumn("year", col("date").substr(1, 4))
date_range_df = date_range_df.withColumn("month", col("date").substr(6, 2))
date_range_df = date_range_df.withColumn("day", col("date").substr(9, 2))
date_range_df = date_range_df.withColumn("day_of_week", date_format(col("date"), "u"))

date_range_df.show()


# COMMAND ----------

(end_date - start_date)

# COMMAND ----------

date_range

# COMMAND ----------

start_date = to_date('1/1/2023','M/dd/yyyy')

# COMMAND ----------

start_date

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = SparkSession.builder.appName("StringToDate").getOrCreate()

# Sample DataFrame with a date string
data = [("1/1/2023",)]
df = spark.createDataFrame(data, ["date_string"])

# Use to_date function to convert the string to a date
df = df.withColumn("date_column", to_date(df["date_string"], "M/d/yyyy"))

df.show()


# COMMAND ----------


