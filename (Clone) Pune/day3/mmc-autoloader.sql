-- Databricks notebook source
-- MAGIC %fs ls dbfs:/mnt/cloudthats3/auto_stream/input/csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC input_path="dbfs:/mnt/cloudthats3/auto_stream/input/csv"
-- MAGIC output_path="dbfs:/mnt/cloudthats3/auto_stream/output/abhishek"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC  .format("cloudFiles")
-- MAGIC  .option("cloudFiles.format","csv")
-- MAGIC  .option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader_csv/schema")
-- MAGIC  .load(f"{input_path}")
-- MAGIC  .writeStream
-- MAGIC  .option("checkpointLocation",f"{output_path}/naval/autoloader_csv/checkpoint")
-- MAGIC  .option("path",f"{output_path}/naval/autoloader_csv/table")
-- MAGIC  .trigger(once=True)
-- MAGIC  .table("my_delta.autoloader_abhishek_csv")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/mnt/cloudthats3/auto_stream/output/abhishek",True)

-- COMMAND ----------

select * from my_delta.autoloader_abhishek_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC  .format("cloudFiles")
-- MAGIC  .option("cloudFiles.format","csv")
-- MAGIC  .option("cloudFiles.schemaLocation",f"{output_path}/abhishek/autoloader_csv/schema")
-- MAGIC  .option("cloudFiles.inferColumnTypes",True)
-- MAGIC  .load(f"{input_path}")
-- MAGIC  .writeStream
-- MAGIC  .option("checkpointLocation",f"{output_path}/abhishek/autoloader_csv/checkpoint")
-- MAGIC  .option("path",f"{output_path}/abhishek/autoloader_csv/table")
-- MAGIC  .table("my_delta.autoloader_abhishek_csv")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC  .format("cloudFiles")
-- MAGIC  .option("cloudFiles.format","json")
-- MAGIC  .option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader_csv/schema")
-- MAGIC  .option("cloudFiles.schemaEvolutionMode","rescue")
-- MAGIC  .option("cloudFiles.inferColumnTypes",True)
-- MAGIC  .load(f"{input_path}")
-- MAGIC  .writeStream
-- MAGIC  .option("checkpointLocation",f"{output_path}/naval/autoloader_csv/checkpoint")
-- MAGIC  .option("path",f"{output_path}/naval/autoloader_csv/table")
-- MAGIC  .option("mergeSchema",True)
-- MAGIC  .trigger(once=True)
-- MAGIC  .table("my_delta.autoloader_abhishek_csv")
-- MAGIC )

-- COMMAND ----------

select * from my_delta.autoloader_abhishek_csv

-- COMMAND ----------


