-- Databricks notebook source
create streaming table sales_bronze as 
select *,current_timestamp() as ingestion_date from cloud_files("dbfs:/mnt/cloudthats3/dlt/sales/","csv",map("cloudFiles.inferColumnTypes","true") )

-- COMMAND ----------

CREATE STREAMING TABLE sales_silver 
(CONSTRAINT valid_order_id EXPECT (order_id is not null) on VIOLATION drop row)
AS select distinct(order_id),customer_id,transaction_id,product_id,quantity,discount_amount,total_amount,order_date from STREAM(LIVE.sales_bronze)

-- COMMAND ----------

create streaming table customers_bronze as 
select *,current_timestamp() as ingestion_date from cloud_files("dbfs:/mnt/cloudthats3/dlt/customers","csv",map("cloudFiles.inferColumnTypes","true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
  FROM STREAM(LIVE.customers_bronze)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, _rescued_data,ingestion_date)
  STORED AS 
  SCD TYPE 2

-- COMMAND ----------

create streaming table products as 
select * from cloud_files("dbfs:/mnt/cloudthats3/dlt/products/","json",map("cloudFiles.inferColumnTypes","true") )
