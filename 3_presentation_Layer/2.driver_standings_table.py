# Databricks notebook source
# MAGIC %run "./../0_Configuration/Folder_Path"

# COMMAND ----------

# MAGIC %md ##1.-Import libraries

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, aggregate

# COMMAND ----------

# MAGIC %md ##2.-Read datasets

# COMMAND ----------

#race_results
driver_standing_df=spark.read.parquet(f"{presentation_folder_path}/race_results")\
    .groupBy("race_year","driver_name", "driver_nationality") \
    .agg(sum("driver_Points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display (driver_standing_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driverstading
