# Databricks notebook source
input_path="dbfs:/mnt/cloudthats3/auto_stream/input/json"
output_path="dbfs:/mnt/cloudthats3/auto_stream/output"

# COMMAND ----------

(
spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader_json/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/naval/autoloader_json/checkpoint")
 .option("path",f"{output_path}/naval/autoloader_json/table")
 .trigger(once=True)
 .table("ny_delta.autoloader_json")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history ny_delta.autoloader_json

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ny_delta.autoloader_json

# COMMAND ----------

(
spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader_json/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/naval/autoloader_json/checkpoint")
 .option("path",f"{output_path}/naval/autoloader_json/table")
 .trigger(once=True)
 .table("ny_delta.autoloader_json")
)
