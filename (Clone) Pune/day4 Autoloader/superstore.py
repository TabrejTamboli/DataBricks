# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/superstore/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/cloudthats3/superstore/",header=True)

# COMMAND ----------

input_path="dbfs:/mnt/cloudthats3/superstore/"

# COMMAND ----------

df=(
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.inferColumnTypes",True)
    .option("cloudFiles.schemaLocation","dbfs:/mnt/cloudthats3/superstoreoutput/naval/schema")
    .load(f"{input_path}")
    )

# COMMAND ----------



# COMMAND ----------

df.createOrReplaceTempView("superstore")

# COMMAND ----------

# MAGIC %sql
# MAGIC create temp view superstore_bronze as
# MAGIC select *,current_timestamp() as ingestion_date, input_file_name() as source_path from superstore

# COMMAND ----------

spark.table("superstore_bronze")
.writeStream
.option("delta.columnMapping.mode","name")
.option("checkpointLocation","dbfs:/mnt/cloudthats3/superstoreoutput/naval/checkpoint")
.table("ny_delta.superstore_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view superstore_bronze
# MAGIC (select Row ID as row_id, Order ID as order_id, Order Date as order_date,Ship Date as ship_date, Ship Mode as ship_mode ,Customer ID as customer_id,Customer Name as customer_name ,Segment,Country/Region as country_region, City,State,Postal Code postal_code,Region,Product ID product_id, Category,Sub-Category,Product Name product_name,Sales,Quantity,Discount,Profit,_rescued_data current_timestamp() as ingestion_date, input_file_name() as source_path from superstore)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view superstore_bronze as 
# MAGIC (select "Row ID" as row_id, "Order ID" as order_id, "Order Date" as order_date,"Ship Date" as ship_date, "Ship Mode" as ship_mode ,"Customer ID" as customer_id,"Customer Name" as customer_name ,Segment ,"Country/Region" as country_region, City, State,"Postal Code" as postal_code, Region, "Product ID" as product_id, Category, Sub-Category, "Product Name" product_name,Sales ,Quantity,Discount,Profit,_rescued_data current_timestamp() as ingestion_date, input_file_name() as source_path from superstore)

# COMMAND ----------


