# Databricks notebook source
# MAGIC %md ###Ingesting results file

# COMMAND ----------

# MAGIC %run "./../0_Configuration/Folder_Path"

# COMMAND ----------

# MAGIC %md ####Paso 1 - Leer el archivo JSON usando el reader de Spark API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

results_schema = StructType(fields=[
                                    StructField("resultId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("grid", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("positionText", StringType(), True),
                                     StructField("positionOrder", IntegerType(), True),
                                     StructField("points", FloatType(), True),
                                     StructField("laps", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True),
                                     StructField("fastestLap", IntegerType(), True),
                                     StructField("rank", IntegerType(), True),
                                     StructField("fastestLapTime", StringType(), True),
                                     StructField("fastestLapSpeed", FloatType(), True),
                                     StructField("statusId", StringType(), True)
                                    ]
                            )
                            

# COMMAND ----------

results_df = spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md ####Paso 2 - Renombrar columnas

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId","result_id") \
                      .withColumnRenamed("raceId","race_Id") \
                      .withColumnRenamed("driverId","driver_Id") \
                      .withColumnRenamed("constructorId","constructor_Id") \
                      .withColumnRenamed("number","drive_number") \
                      .withColumnRenamed("positionOrder","position_Order") \
                      .withColumnRenamed("points","driver_Points") \
                      .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md ####Paso 3 - Eliminar columnas no deseadas

# COMMAND ----------

cols_to_remove = ["statusId", "rank"]
results_final_df= results_with_columns_df.drop(*cols_to_remove)

# COMMAND ----------

# MAGIC %md ####Paso 4 - Cargar resultado en un parquet

# COMMAND ----------

results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

results_final_df.write.mode("overwrite").parquet(processed_folder_path + "/resultsFile") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results
