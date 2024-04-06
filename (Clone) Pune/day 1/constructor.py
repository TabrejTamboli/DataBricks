# Databricks notebook source
df=spark.read.json("dbfs:/FileStore/tables/formula1_raw/constructors.json")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/formula1_raw

# COMMAND ----------

from pyspark.sql.functions import *
df_final=df.withColumn("ingestion_date",current_timestamp()).drop("url")

# COMMAND ----------

df_final.write.saveAsTable("naval.constructor2")

# COMMAND ----------

(spark
.read\
.json("dbfs:/FileStore/tables/formula1_raw/constructors.json")
.withColumn("ingestion_date",current_timestamp())\
.drop("url")\
.write\
.mode("overwrite")\
.saveAsTable("naval.constructor2"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from file_format.`path`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table naval.constructor3 as
# MAGIC select *,current_timestamp() as ingestion_date from json.`dbfs:/FileStore/tables/formula1_raw/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.constructor3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/FileStore/tables/formula1_raw/circuits.csv`

# COMMAND ----------


