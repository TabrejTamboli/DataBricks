# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

https://spark.apache.org/docs/latest/api/python/reference/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting path

# COMMAND ----------

# MAGIC %fs  ls dbfs:/FileStore/tables/

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1: Extract csv into dataframe

# COMMAND ----------



# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP 2: Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC Dataframe Functions
# MAGIC - select 
# MAGIC - alias
# MAGIC - withColumnRenamed
# MAGIC - columns
# MAGIC - toDF
# MAGIC - drop
# MAGIC - withColumn
# MAGIC
# MAGIC Functions
# MAGIC - col
# MAGIC - concat
# MAGIC - lit

# COMMAND ----------

help(df.select)

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select("circuitId","circuitRef").display()

# COMMAND ----------

df.select("circuitId".alias("circuit_id")).display()

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

df.select("circuitId",col("circuitRef"),df.name,df["location"]).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.select(concat("location"," ","country")).display()

# COMMAND ----------

df.select(concat("location",lit(" "),"country").alias("loc&country")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").display()

# COMMAND ----------

df.columns

# COMMAND ----------

new_column_names=['circuit_id',
 'circuit_ref',
 'name',
 'location',
 'country',
 'latitude',
 'longtitude',
 'altitude',
 'url']

# COMMAND ----------

df.toDF(new_column_names)

# COMMAND ----------

df1=df.toDF(*new_column_names)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.drop("url").display()

# COMMAND ----------

df1.withColumn("current_time",current_date()).display()

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

df1.withColumn("upper_name",upper(col("name"))).display()

# COMMAND ----------

df1.write.parquet("dbfs:/FileStore/tables/output/naval/circuit")

# COMMAND ----------

df1.write.mode("overwrite").parquet("dbfs:/FileStore/tables/output/naval/circuit")

# COMMAND ----------

df=spark.read.parquet("dbfs:/FileStore/tables/output/naval/circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists naval

# COMMAND ----------

# MAGIC   %sql 
# MAGIC select location,count(location) as count from amey.circuit group by location

# COMMAND ----------



# COMMAND ----------


