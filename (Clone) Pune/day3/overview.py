# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/cloudthats3/formula1_raw/races.csv",header=True,inferSchema=True)

# COMMAND ----------

df.write.format("parquet").save("dbfs:/mnt/cloudthats3/output/parquet")

# COMMAND ----------

df.write.format("delta").save("dbfs:/mnt/cloudthats3/output/delta")

# COMMAND ----------

df.write.save("dbfs:/mnt/cloudthats3/output/delta_default")

# COMMAND ----------

df.write.saveAsTable("naval.results_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended naval.results_new

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history naval.results_new

# COMMAND ----------

df.write.option("path","dbfs:/mnt/cloudthats3/output/results_table").saveAsTable("naval.results_new_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table naval.results_new

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table naval.results_new_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/cloudthats3/output/results_table`

# COMMAND ----------

df=spark.read.delta("dbfs:/mnt/cloudthats3/output/results_table")

# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/mnt/cloudthats3/output/results_table")

# COMMAND ----------


