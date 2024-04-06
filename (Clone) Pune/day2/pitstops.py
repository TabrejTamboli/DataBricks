# Databricks notebook source
# MAGIC %md
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html

# COMMAND ----------

# MAGIC %run /Workspace/Users/naval@cloudthat.net/Pune/day2/includes

# COMMAND ----------

df=spark.read.json(f"{input_raw_files}/pit_stops.json",multiLine=True)

# COMMAND ----------



# COMMAND ----------


