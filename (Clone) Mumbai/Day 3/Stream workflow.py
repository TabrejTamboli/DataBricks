# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@cloudthat.net/Day 3/include"

# COMMAND ----------

users_stream= "Id int, Name string, Gender string, Salary int, Country string, Date string"

# COMMAND ----------

(spark
.readStream
.schema(users_stream)
.csv(f"{input_stream}",header=True)
.writeStream
.option("checkpointLocation",f"{output_stream}naval/emp/checkpoint")
.trigger(once=True)
.toTable("naval.firststream"))
