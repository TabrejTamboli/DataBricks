-- Databricks notebook source
-- MAGIC %fs ls dbfs:/mnt/cloudthats3/dlt/customers/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC input_path="dbfs:/mnt/cloudthats3/dlt/customers/"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv(f"{input_path}")

-- COMMAND ----------


schema="naval"

-- COMMAND ----------


select * from '${schema}'.climate_25
