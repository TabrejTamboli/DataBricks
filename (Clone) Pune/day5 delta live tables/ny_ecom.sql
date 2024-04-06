-- Databricks notebook source
Create streaming live table sales_bronze
as
select *, current_timestamp() as ingestion_date,input_file_name() as source_path from cloud_files("dbfs:/mnt/cloudthats3/dlt/sales/", "csv",map("cloudFiles.inferColumnTypes","True") )

-- COMMAND ----------

create streaming live table sales_silver
(constraint valid_order_id expect(order_id IS NOT Null) on violation Drop row)
as
select DISTINCT order_id,customer_id,transaction_id,product_id,quantity,total_amount,order_date from STREAM (LIVE.sales_bronze)

-- COMMAND ----------

Create streaming live table customers_bronze
as
select *,current_timestamp() as ingestion_date,input_file_name() as source_path from cloud_files("dbfs:/mnt/cloudthats3/dlt/customers/", "csv" )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
  FROM STREAM(LIVE.customers_bronze)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation,sequenceNum, _rescued_data,ingestion_date,source_path)
  STORED AS
  SCD TYPE 2

-- COMMAND ----------

Create streaming live table product_bronze
as
select *,current_timestamp() as ingestion_date,input_file_name() as source_path from cloud_files("dbfs:/mnt/cloudthats3/dlt/products/", "csv" )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE products_silver;

APPLY CHANGES INTO LIVE.products_silver
  FROM STREAM(LIVE.product_bronze)
  KEYS (product_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY seqNum
  COLUMNS * EXCEPT (operation,seqNum, _rescued_data,ingestion_date,source_path)
  STORED AS
  SCD TYPE 1

-- COMMAND ----------

Create streaming live table order_date_bronze
as
select * from cloud_files("dbfs:/mnt/cloudthats3/dlt/order_date/", "csv" )
