# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/raw/

# COMMAND ----------

df=spark.read.format("csv").load("dbfs:/mnt/cloudthats3/raw/emp.xlsx")

# COMMAND ----------

display(df)

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.pandas import read_excel

df_excel = read_excel('dbfs:/mnt/cloudthats3/raw/emp.xlsx')

display(df_excel.filter(df_excel.emp_id == 1))

# COMMAND ----------

df=spark.read.format("com.crealytics.spark.excel").load("dbfs:/mnt/cloudthats3/raw/emp.xlsx",header=True)

# COMMAND ----------

df.display()

# COMMAND ----------


