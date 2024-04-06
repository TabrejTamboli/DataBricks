# Databricks notebook source
ETL 
ELT

csv
df= spark.read.csv("path")

parquet, simple json
CTAS


L/save/ write 
dw =analytics
dl=machine
---------------
Lakehouse = (DW + DL)
delta 

# COMMAND ----------

By Default databricks save in DELTA

FILES
df.write.parquet("path")  
df.write.format("parquet").save("path")
df.write.format("csv").save("path")
df.write.format("delta").save("path")
df.write.save("path")

Table
df.write.format("csv").option("path","/mnt/").saveAsTable("tablename")
df.write.format("parquet").saveAsTable("tablename")
df.write.format("delta").saveAsTable("tablename")
df.write.saveAsTable("tablename")


Final:
Files:
df.write.save("path")----delta file format
table
df.write.saveAsTable("tblname")--- delta table 

