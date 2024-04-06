# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@cloudthat.net/day 1/includes"

# COMMAND ----------

input_path

# COMMAND ----------

name="nikhil"
dept="data engineering"

# COMMAND ----------

print(f"I am {name} working as a {dept}")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

df2=spark.read.json("dbfs:/FileStore/tables/raw/iot1.json")

# COMMAND ----------

df2.write.saveAsTable("naval.iotdata")

# COMMAND ----------

df=spark.read.json(f"{input_path}iot1.json")

# COMMAND ----------

df=spark.read.json(f"{input_path}emp.csv")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------


