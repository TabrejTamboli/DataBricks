# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@cloudthat.net/day 1/includes"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/naval@cloudthat.net/day 1/functions"

# COMMAND ----------

dbutils.widgets.text("parameter1","")
variable_para=dbutils.widgets.get("parameter1")

# COMMAND ----------

df=spark.read.csv(f"{input_path}circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df1=add_ingestion(df)

# COMMAND ----------

df2=df1.withColumn("envrionment",lit(variable_para))

# COMMAND ----------

df2.write.mode("overwrite").saveAsTable("naval.circuit_final")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.circuit_final

# COMMAND ----------



# COMMAND ----------


