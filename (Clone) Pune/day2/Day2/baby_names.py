# Databricks notebook source


# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/cloudthats3/raw/Baby_Names.csv",header=True,inferSchema=True)

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy("Year").count().sort("Year").display()

# COMMAND ----------

df.write.parquet("dbfs:/mnt/cloudthats3/output_formula1/naval/baby_names")

# COMMAND ----------

df.write.partitionBy("Year").parquet("dbfs:/mnt/cloudthats3/output_formula1/naval/baby_names_by_year")

# COMMAND ----------

df.write.partitionBy("Year","Sex").parquet("dbfs:/mnt/cloudthats3/output_formula1/naval/baby_names_by_year_gender")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/cloudthats3/output_formula1/naval/baby_names` where Year= 2008

# COMMAND ----------


