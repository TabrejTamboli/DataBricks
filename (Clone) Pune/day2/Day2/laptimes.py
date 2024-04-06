# Databricks notebook source


# COMMAND ----------

# MAGIC %run /Workspace/Users/naval@cloudthat.net/Pune/day2/includes

# COMMAND ----------

lap_schema="raceId int, driverID int, lap int, position int, time string, millisecond int"

# COMMAND ----------

df=spark.read.csv(f"{input_raw_files}/lap_times/",inferSchema=True)

# COMMAND ----------

df=spark.read.schema(lap_schema).csv(f"{input_raw_files}/lap_times/")

# COMMAND ----------

display(df)

# COMMAND ----------


