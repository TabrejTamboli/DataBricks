# Databricks notebook source
help(spark.createDataFrame)

# COMMAND ----------

data=([(1,'a',30),(2,'b',34)])
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

Files
(csv,json,xml,parquet,avro, delta,orc,audio, video,table)

(S3,blob, adls, gcp, database, datawareshouse)

# COMMAND ----------

DBFS:
    Databricks file system: abstraction of your cloud storage, linux 

# COMMAND ----------

https://docs.databricks.com/en/dbfs/index.html#what-is-the-databricks-file-system-dbfs

# COMMAND ----------

dbutilites
dbutils

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/processed")

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/processed/emp.csv","dbfs:/FileStore/tables/raw/")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/processed")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

https://spark.apache.org/docs/latest/api/python/reference/index.html

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/raw/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()
df.printSchema()

# COMMAND ----------

df.write.saveAsTable("naval.emp_demp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.emp_demp where id= 3

# COMMAND ----------


