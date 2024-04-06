# Databricks notebook source
catalog_schema="hive_metastore.naval"

# COMMAND ----------

df=spark.read.table(f"{catalog_schema}.circuit_final")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${catalog_schema}.circuit_final

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${catalog_schema}.employee where id=${emp_id}

# COMMAND ----------


