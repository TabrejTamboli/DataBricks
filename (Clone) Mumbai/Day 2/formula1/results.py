# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@cloudthat.net/Day 2/formula1/includes"

# COMMAND ----------

(spark.read
 .json(f"{input_path}results.json")
 .write.mode("overwrite")
 .format("parquet")
 .save(f"{output_path}naval/results"))
