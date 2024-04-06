# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/raw/emp.xlsx

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/mnt/cloudthats3/raw/emp.xlsx")

# COMMAND ----------

df.display()

# COMMAND ----------



# COMMAND ----------

df=spark.read.format("com.crealytics.spark.excel").load("dbfs:/mnt/cloudthats3/raw/emp.xlsx",header=True)

# COMMAND ----------

df.display()

# COMMAND ----------


