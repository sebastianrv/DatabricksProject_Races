# Databricks notebook source
# MAGIC %md ###Ingesting results file

# COMMAND ----------

# MAGIC %run "./../0_Configuration/Folder_Path"

# COMMAND ----------

# MAGIC %md ####Paso 1 - Leer el archivo JSON usando el reader de Spark API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, StringType, FloatType

# COMMAND ----------

qualifying_schema = StructType(fields=[
                                    StructField("qualifyId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("q1", StringType(), True),
                                     StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True)
                                    ]
                            )
                            

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.option("mode", "PERMISSIVE")\
.json(f"{raw_folder_path}/qualifying/*")

# COMMAND ----------

# MAGIC %md ####Paso 2 - Renombrar columnas

# COMMAND ----------

qualifying_with_columns_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
                      .withColumnRenamed("raceId","race_Id") \
                      .withColumnRenamed("driverId","driver_Id") \
                      .withColumnRenamed("constructorId","constructor_Id") \
                      .withColumnRenamed("number","drive_number") \
                      .withColumnRenamed("position","qualifying_position") \
                      .withColumnRenamed("points","driver_Points")

# COMMAND ----------

# MAGIC %md ####Paso 3 - Eliminar columnas no deseadas

# COMMAND ----------

#qualifying_final_df= qualifying_with_columns_df.drop(col("statusId","rank"))

# COMMAND ----------

# MAGIC %md ####Paso 4 - Cargar resultado en un parquet

# COMMAND ----------

qualifying_with_columns_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

qualifying_with_columns_df.write.mode("overwrite").parquet(processed_folder_path + "/qualifyingFile") 
