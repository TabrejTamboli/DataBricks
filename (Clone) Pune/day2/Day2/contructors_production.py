# Databricks notebook source
# MAGIC %run /Workspace/Users/naval@cloudthat.net/Pune/day2/includes

# COMMAND ----------

df=spark.read.json(f"{input_raw_files}/constructors.json")

# COMMAND ----------

df_final=df.drop("url").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{output_path}naval/constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select constructorId as constructor_id,constructorRef,name,nationality,current_timestamp() as ingestion_date from json.`dbfs:/mnt/cloudthats3/formula1_raw/constructors.json`

# COMMAND ----------

# df=spark.read.parquet(f"{output_path}naval/constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from parquet.`dbfs:/mnt/cloudthats3/output_formula1/naval/constructor`

# COMMAND ----------

# df=spark.read.parquet(f"{output_path}naval/circuit")
