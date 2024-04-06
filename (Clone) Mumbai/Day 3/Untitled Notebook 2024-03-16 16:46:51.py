# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@cloudthat.net/day 1/includes"

# COMMAND ----------

df=spark.read.csv(f"{input_path}circuits.csv",header=True)

# COMMAND ----------

df.explain()

# COMMAND ----------

df.filter("circuitID=1").display()

# COMMAND ----------

df.groupBy("country").count().explain()

# COMMAND ----------

df.groupBy("country").count().display()

# COMMAND ----------


