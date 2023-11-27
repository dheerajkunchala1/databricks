-- Databricks notebook source
USE CATALOG covid_ecdc;

-- COMMAND ----------

DROP SCHEMA IF EXISTS lookup;
CREATE SCHEMA lookup;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC # Function to calculate the week of the month
-- MAGIC # Function to get week of the year
-- MAGIC def week_of_year(dt):
-- MAGIC     first_day = datetime(dt.year, dt.month, 1)
-- MAGIC     adjusted_first_day = first_day + timedelta(days=(7 - first_day.weekday()))
-- MAGIC     return (dt - adjusted_first_day).days // 7 + 2 if adjusted_first_day.month < dt.month else 1
-- MAGIC
-- MAGIC # Register the function as a UDF
-- MAGIC spark.udf.register("week_of_year_udf", week_of_year)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC def week_of_month(date):
-- MAGIC     first_day = date.replace(day=1)
-- MAGIC     adjusted_first_day = first_day + timedelta(days=(7 - first_day.weekday()))
-- MAGIC     if adjusted_first_day.month < date.month:
-- MAGIC         return 1
-- MAGIC     else:
-- MAGIC         return (date.day + adjusted_first_day.weekday() - 1) // 7 + 1
-- MAGIC       
-- MAGIC # Register the function as a UDF
-- MAGIC spark.udf.register("week_of_month_udf", week_of_month)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC def week_of_year_with_year(date):
-- MAGIC     # Define the starting point as the first day of the year
-- MAGIC     start_date = datetime(date.year, 1, 1)
-- MAGIC
-- MAGIC     # Calculate the number of days between the date and the starting point
-- MAGIC     days = (date - start_date).days
-- MAGIC
-- MAGIC     # Calculate the week number, adding 1 to begin from week 1
-- MAGIC     week = (days // 7) + 1
-- MAGIC
-- MAGIC     # Construct the 'YYYYWW' format
-- MAGIC     return f"{date.year}{week:02d}"
-- MAGIC
-- MAGIC # Register the function as a UDF
-- MAGIC spark.udf.register("week_of_year_with_year_udf", week_of_year_with_year)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC some_date = datetime.strptime('2023-01-01', '%Y-%m-%d')
-- MAGIC result = week_of_year_with_year(some_date)
-- MAGIC print(result)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import col, date_format, year, month, dayofmonth, dayofweek, weekofyear, quarter,expr, to_date,  dayofyear
-- MAGIC from datetime import datetime, timedelta

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import col, date_format, year, month, dayofmonth, dayofweek, weekofyear, quarter,expr, to_date,  dayofyear
-- MAGIC from datetime import datetime, timedelta
-- MAGIC
-- MAGIC # Create SparkSession
-- MAGIC spark = SparkSession.builder.appName("CalendarDimension").getOrCreate()
-- MAGIC
-- MAGIC # Generate a range of dates for a certain period
-- MAGIC start_date = datetime.strptime('2001-01-01', '%Y-%m-%d')
-- MAGIC end_date = datetime.strptime('2030-12-31', '%Y-%m-%d')
-- MAGIC
-- MAGIC date_list = []
-- MAGIC delta = timedelta(days=1)
-- MAGIC while start_date <= end_date:
-- MAGIC     date_list.append(start_date.strftime("%Y-%m-%d"))
-- MAGIC     start_date += delta
-- MAGIC
-- MAGIC # Create a DataFrame from the list of dates
-- MAGIC date_df = spark.createDataFrame(date_list, "string").toDF("date")
-- MAGIC
-- MAGIC # Extract various date-related columns
-- MAGIC calendar_df = date_df \
-- MAGIC     .select(
-- MAGIC         col("date").alias("full_date"),
-- MAGIC         date_format(col("date"), 'yyyyMMdd').alias("date_key"),
-- MAGIC         year(col("date")).alias("year"),
-- MAGIC         month(col("date")).alias("month"),
-- MAGIC         dayofmonth(col("date")).alias("day"),
-- MAGIC         dayofweek(col("date")).alias("day_of_week"),
-- MAGIC         dayofyear(col("date")).alias("day_of_year"),
-- MAGIC         expr("week_of_year_udf(to_date(date))").cast("int").alias("week_of_year"),
-- MAGIC         expr("week_of_month_udf(to_date(date))").cast("int").alias("week_of_month"),
-- MAGIC         quarter(col("date")).alias("quarter"),
-- MAGIC         date_format(col("date"), 'MMMM').alias("month_name"),
-- MAGIC         date_format(col("date"), 'E').alias("day_name"),
-- MAGIC         date_format(col("date"), 'yyyy').cast("long").alias("calendar_year"),
-- MAGIC         date_format(col("date"), 'yyyyMM').alias("year_month")
-- MAGIC       #  expr("week_of_year_with_year_udf(to_date(date))").cast("int").alias("week_of_year")
-- MAGIC     )
-- MAGIC
-- MAGIC # Show the calendar dimension table
-- MAGIC calendar_df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC calendar_df.printSchema()

-- COMMAND ----------

DROP TABLE IF EXISTS covid_ecdc.lookup.calendar;
CREATE TABLE covid_ecdc.lookup.calendar
(
date_key STRING,
full_date STRING,
calendar_year BIGINT,
day INT,
quarter INT,
day_name STRING,
day_of_week INT,
day_of_year INT,
week_of_month INT,
week_of_year INT,
month_name STRING,
year_month STRING
--year_week STRING
)
USING delta


-- COMMAND ----------

-- MAGIC %python
-- MAGIC calendar_df.write.option("mergeSchema", "true").saveAsTable("covid_ecdc.lookup.calendar", mode="overwrite")

-- COMMAND ----------

SELECT * from covid_ecdc.lookup.calendar;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC calendar_df.write.format("csv").options(header=True).mode('overwrite').save('abfss://ecdc@covidreportingdl911.dfs.core.windows.net/lookup')

-- COMMAND ----------

SELECT * from covid_ecdc.lookup.calendar
where date_key = '20200401'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import col, sum
-- MAGIC
-- MAGIC # Initialize Spark session
-- MAGIC spark = SparkSession.builder.appName("NullCountExample").getOrCreate()
-- MAGIC
-- MAGIC # Assuming 'df' is your DataFrame
-- MAGIC # Replace 'your_table' with the actual table or DataFrame name
-- MAGIC
-- MAGIC # Create a DataFrame with the counts of null values in each column
-- MAGIC null_counts = calendar_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in calendar_df.columns])
-- MAGIC
-- MAGIC # Show the result
-- MAGIC null_counts.show(truncate=False)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df2.show()

-- COMMAND ----------


