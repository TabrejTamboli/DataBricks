# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/auto_stream/input/csv

# COMMAND ----------

input_path="dbfs:/mnt/cloudthats3/auto_stream/input/csv"
output_path="dbfs:/mnt/cloudthats3/auto_stream/output"

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader_csv/schema")
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/naval/autoloader_csv/checkpoint")
 .option("path",f"{output_path}/naval/autoloader_csv/table")
 .trigger(once=True)
 .table("ny_delta.autoloader_csv")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ny_delta.autoloader_csv

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader_csv/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/naval/autoloader_csv/checkpoint")
 .option("path",f"{output_path}/naval/autoloader_csv/table")
 .table("ny_delta.autoloader_csv")
)

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader_csv/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/naval/autoloader_csv/checkpoint")
 .option("path",f"{output_path}/naval/autoloader_csv/table")
 .table("ny_delta.autoloader_csv")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ny_delta.autoloader_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table ny_delta.autoloader_csv

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader_csv/schema")
 .option("cloudFiles.schemaEvolutionMode","rescue")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/naval/autoloader_csv/checkpoint")
 .option("path",f"{output_path}/naval/autoloader_csv/table")
 .option("mergeSchema",True)
 .table("ny_delta.autoloader_csv")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ny_delta.autoloader_csv

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader_csv2/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/naval/autoloader_csv2/checkpoint")
 .option("path",f"{output_path}/naval/autoloader_csv2/table")
 .table("ny_delta.autoloader_csv2")
)

# COMMAND ----------

  (spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader_csv2/schema")
 .option("cloudFiles.schemaEvolutionMode","rescue")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f"{input_path}")
 .writeStream
 .option("checkpointLocation",f"{output_path}/naval/autoloader_csv2/checkpoint")
 .option("path",f"{output_path}/naval/autoloader_csv2/table")
 .table("ny_delta.autoloader_csv2")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ny_delta.autoloader_csv2

# COMMAND ----------


