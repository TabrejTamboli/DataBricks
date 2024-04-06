# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@cloudthat.net/Day 2/formula1/includes"

# COMMAND ----------

df_laptimes=spark.read.schema(laptimes_schema).csv(f"{input_path}lap_times/",header=True)

# COMMAND ----------

df_laptimes.write.mode("overwrite").partitionBy("driverId").parquet(f"{output_path}/naval/laptime")

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

output_path

# COMMAND ----------

df_laptimes.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/cloudthats3/formula1_processed_parquet/naval/laptime` where raceId= 1046

# COMMAND ----------

df_laptimes.count()

# COMMAND ----------

df_laptimes.groupBy("raceId").count().display()

# COMMAND ----------

df_laptimes.groupBy("driverID").count().display()

# COMMAND ----------


