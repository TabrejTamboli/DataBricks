# Databricks notebook source
batch

read
df=spark.read.csv("path")
write
df.write.saveAsTable("path")


streaming
read
df=spark.readStream.csv("path")
write
df.writeStream.option("checkpointloction","path").table("tablename")


# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/stream/input/csv_raw/emp/

# COMMAND ----------

user_schema="Id int, Name string, Gender string, Salary int, Country string, Date string"

# COMMAND ----------

df=spark.readStream.schema(user_schema).csv("dbfs:/mnt/cloudthats3/stream/input/csv_raw/emp/",header=True)

# COMMAND ----------

df.writeStream.option("checkpointLocation","dbfs:/mnt/cloudthats3/stream/stream_pune/naval/stream_first/checkpiont").table("naval.stream_first%")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.stream_first

# COMMAND ----------

(spark
 .readStream
 .schema(user_schema)
 .csv("dbfs:/mnt/cloudthats3/stream/input/csv_raw/emp/",header=True)
    .writeStream
    .option("checkpointLocation","dbfs:/mnt/cloudthats3/stream/stream_pune/naval/stream_first/checkpiont")
    .trigger(availableNow=True)
    .table("naval.stream_first"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.stream_first

# COMMAND ----------


