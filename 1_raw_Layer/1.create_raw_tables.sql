-- Databricks notebook source
-- MAGIC %md ##Configuration

-- COMMAND ----------

--%python
--%run "./../0_Configuration/ADLS_Connection"
--%run "./../0_Configuration/Create_Database"

-- COMMAND ----------

-- MAGIC %md ##Create tables for CSV files

-- COMMAND ----------

-- MAGIC %md ###Create Circuits Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId	INT,
  circuitRef STRING,	
  name STRING,
  location STRING,
  country STRING,
  lat	DOUBLE,
  lng	DOUBLE,
  alt	INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/sarsv2023/blob-raw/circuits.csv", header true);

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md ###Create Races Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/sarsv2023/blob-raw/races.csv", header true, delimiter ',');

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md ###Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md ###Create Constructors Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/sarsv2023/blob-raw/constructors.json");

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md ###Create Drivers Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/sarsv2023/blob-raw/drivers.json");

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md ###Create Qualifying Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
 qualifyId INT,
 raceId INT,
 driverId INT,
 constructorId INT,
 number INT,
 position INT,
 q1 STRING,
 q2 STRING,
 q3 STRING
)
USING json
OPTIONS (path "/mnt/sarsv2023/blob-raw/qualifying/",multiline=true);

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

-- MAGIC %md ###Create results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
 resultId INT,
 raceId INT,
 driverId INT,
 constructorId INT,
 number INT,
 grid INT,
 position INT,
 positinText STRING,
 positionOrder INT,
 points FLOAT,
 laps INT,
 time STRING,
 miliseconds INT,
 fastestLap INT,
 rank INT,
 fastestLapTime STRING,
 fastestLapSpeed STRING,
 statusId INT
)
USING json
OPTIONS (path "/mnt/sarsv2023/blob-raw/results.json");

-- COMMAND ----------

select * from f1_raw.results
