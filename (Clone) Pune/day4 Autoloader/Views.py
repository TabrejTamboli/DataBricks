# Databricks notebook source
Std( SQL)
Temp View(SQL, pyspark)
Global Temp View(SQL, pyspark)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW naval.climate25 as select * from hive_metastore.naval.climate_1 where Temperature > 25

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW climate25_temp as select * from hive_metastore.naval.climate_1 where Temperature < 25

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from climate25_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE GLOBAL TEMP VIEW climate25_global as select * from hive_metastore.naval.climate_1 where Temperature < 25

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in naval

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.climate25_global

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("bikesharing")

# COMMAND ----------

# MAGIC %sql
# MAGIC CReate table naval.bikesharing1 as 
# MAGIC select *, current_timestamp() as ingestion_date, input_file_name() as source_path from bikesharing

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.bikesharing1

# COMMAND ----------


