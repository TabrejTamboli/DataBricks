# Databricks notebook source
# MAGIC %run /Workspace/Users/naval@cloudthat.net/Pune/day2/includes

# COMMAND ----------

df=spark.read.csv(f"{input_raw_files}/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df_final=df.drop("url").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{output_path}naval/circuit")
