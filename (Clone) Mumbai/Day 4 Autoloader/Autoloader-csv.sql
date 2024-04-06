-- Databricks notebook source
-- MAGIC %fs ls dbfs:/mnt/cloudthats3/stream/input/json_raw/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC input_path="dbfs:/mnt/cloudthats3/stream/input/csv_raw/emp_auto"
-- MAGIC output_path="dbfs:/mnt/cloudthats3/stream/output"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC  .format("cloudFiles")
-- MAGIC  .option("cloudFiles.format","csv")
-- MAGIC  .load(f"{input_path}")
-- MAGIC  .writeStream
-- MAGIC  .option("checkpointLocation",f"{output_path}/naval/emp_autoloader")
-- MAGIC  .table("naval.emp_autoloader")
-- MAGIC  )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC  .format("cloudFiles")
-- MAGIC  .option("cloudFiles.format","csv")
-- MAGIC  .option("cloudFiles.schemaLocation",f"{output_path}/naval/emp_autoloader/schemalocation")
-- MAGIC  .load(f"{input_path}")
-- MAGIC  .writeStream
-- MAGIC  .option("checkpointLocation",f"{output_path}/naval/emp_autoloader/checkpoint")
-- MAGIC  .table("naval.emp_autoloader")
-- MAGIC  )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table("naval.emp_autoloader")

-- COMMAND ----------

select * from naval.emp_autoloader

-- COMMAND ----------

cloudFiles.inferColumnTypes

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC  .format("cloudFiles")
-- MAGIC  .option("cloudFiles.format","csv")
-- MAGIC  .option("cloudFiles.inferColumnTypes",True)
-- MAGIC  .option("cloudFiles.schemaLocation",f"{output_path}/naval/emp_autoloader/schemalocation")
-- MAGIC  .load(f"{input_path}")
-- MAGIC  .writeStream
-- MAGIC  .option("checkpointLocation",f"{output_path}/naval/emp_autoloader/checkpoint")
-- MAGIC  .table("naval.emp_autoloader")
-- MAGIC  )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table("naval.emp_autoloader")

-- COMMAND ----------

select * from naval.emp_autoloader

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC  .format("cloudFiles")
-- MAGIC  .option("cloudFiles.format","csv")
-- MAGIC  .option("cloudFiles.inferColumnTypes",True)
-- MAGIC  .option("cloudFiles.schemaLocation",f"{output_path}/naval/emp_autoloader/schemalocation")
-- MAGIC  .load(f"{input_path}")
-- MAGIC  .writeStream
-- MAGIC  .option("checkpointLocation",f"{output_path}/naval/emp_autoloader/checkpoint")
-- MAGIC  .table("naval.emp_autoloader")
-- MAGIC  )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC  .format("cloudFiles")
-- MAGIC  .option("cloudFiles.format","csv")
-- MAGIC  .option("cloudFiles.inferColumnTypes",True)
-- MAGIC  .option("cloudFiles.schemaLocation",f"{output_path}/naval/emp_autoloader/schemalocation")
-- MAGIC  .option("cloudFiles.schemaEvolutionMode","rescue")
-- MAGIC  .load(f"{input_path}")
-- MAGIC  .writeStream
-- MAGIC  .option("checkpointLocation",f"{output_path}/naval/emp_autoloader/checkpoint")
-- MAGIC  .option("mergeSchema",True)
-- MAGIC  .table("naval.emp_autoloader")
-- MAGIC  )

-- COMMAND ----------

select * from naval.emp_autoloader

-- COMMAND ----------

describe history naval.emp_autoloader

-- COMMAND ----------


