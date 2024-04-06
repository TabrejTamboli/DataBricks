# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@cloudthat.net/Day 3/include"

# COMMAND ----------

input_stream

# COMMAND ----------

df=spark.readStream.csv(f"{input_stream}")

# COMMAND ----------

users_stream= "Id int, Name string, Gender string, Salary int, Country string, Date string"

# COMMAND ----------

df=spark.readStream.schema(users_stream).csv(f"{input_stream}",header=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.writeStream.toTable("naval.firststream")

# COMMAND ----------

df.writeStream.option("checkpointLocation","dbfs:/mnt/cloudthats3/stream/output/naval/emp/checkpoint").toTable("naval.firststream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.naval.firststream

# COMMAND ----------

(spark
.readStream
.schema(users_stream)
.csv(f"{input_stream}",header=True)
.writeStream
.option("checkpointLocation",f"{output_stream}naval/emp/checkpoint")
.trigger(once=True)
.toTable("naval.firststream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.naval.firststream

# COMMAND ----------


