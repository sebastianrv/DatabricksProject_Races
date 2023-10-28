# Databricks notebook source
# MAGIC %run "./../0_Configuration/Folder_Path"

# COMMAND ----------

# MAGIC %md ##1.-Import libraries

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md ##2.-Read datasets

# COMMAND ----------

#races
races_df=spark.read.parquet(f"{processed_folder_path}/races")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("race_timestamp","race_date")\
    #.select("race_year","race_name","race_date")

#circuits
circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("location","circuit_location")\
    #.select("circuit_location")

#drivers
drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")
    #.select("circuit_location")

#constructors
constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors")\
    .withColumnRenamed("name","team")

#results
results_df=spark.read.parquet(f"{processed_folder_path}/results")\
    .withColumnRenamed("time","time_id")\
    .withColumnRenamed("race_id","result_race_id")\
    .withColumnRenamed("file_date","result_file_date")\

#races_df=spark.table("f1_proccesed.races")
#display (races_df)

# COMMAND ----------

# MAGIC %md ##3.-Join df

# COMMAND ----------

race_circuits_df=races.df.join(
        circuits_df,
        (races_df.circuit_id==circuits_df.circuit_id),
        "inner")\
        .select(
        races_df.race_id,
        races_df.race_year,
        races_df.race_name,
        races_df.race_date,
        circuits_df.circuits_location
                )


# COMMAND ----------

race_results_df=results_df.join(race_circuits_df,results_df.result_race_id==race_circuits_df.race_id)\
    .join(drivers_df,results_df.driver_id==drivers_df.driver_id)\
    .join(constructor_df,results_df.constructors_id==constructor_df.constructors_id)

# COMMAND ----------

final_df=race_results_df\
    .select("race_idf","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position")\
    .withColumn("created_date",current_timestamp())
    #.withColumnRenamed("result_file_date","file_date")    

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results

# COMMAND ----------

Raw:
-Circuits
-Races
-Constructors
-Drivers
-Qualifying
-LapTimes (borrar)

driverStandings


