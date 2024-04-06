# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@cloudthat.net/Pune/day3/prod- includes"

# COMMAND ----------

# MAGIC %run /Workspace/Users/naval@cloudthat.net/Pune/day3/functions

# COMMAND ----------

dbutils.widgets.text("enviroment"," ")
env=dbutils.widgets.get("enviroment")

# COMMAND ----------

df=spark.read.json(f"{input_path}",multiLine=True)

# COMMAND ----------

df2=add_column(df)

# COMMAND ----------

df_final=df2.withColumn("environment",lit(env))

# COMMAND ----------

df_final.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("naval.finance")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.finance
