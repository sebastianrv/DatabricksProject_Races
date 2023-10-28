-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw
LOCATION "/mnt/sarsv2023/blob-raw"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/sarsv2023/blob-silver"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/sarsv2023/blob-gold"
