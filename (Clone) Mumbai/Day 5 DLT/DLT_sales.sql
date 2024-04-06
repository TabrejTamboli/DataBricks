-- Databricks notebook source
create streaming live table sales_bronze
COMMENT "The Sales"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
as 
select *,current_timestamp() as ingestion_date, input_file_name() as source_path from cloud_files("dbfs:/mnt/cloudthats3/dlt/sales","csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

create streaming table sales_silver
(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The Sales Silver"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
as
select distinct(order_id), customer_id,transaction_id,product_id,quantity,discount_amount,total_amount,order_date  from STREAM(LIVE.sales_bronze)

-- COMMAND ----------

create streaming live table customers_bronze
COMMENT "The customer data"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
as 
select *,current_timestamp() as ingestion_date, input_file_name() as source_path from cloud_files("dbfs:/mnt/cloudthats3/dlt/customers","csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

create streaming live table product_bronze
COMMENT "The customer data"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
as 
select *,current_timestamp() as ingestion_date, input_file_name() as source_path from cloud_files("dbfs:/mnt/cloudthats3/dlt/products","csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

-- Create and populate the target table.
CREATE OR REFRESH STREAMING TABLE customer_silver;

APPLY CHANGES INTO
  live.customer_silver
FROM
  stream(LIVE.customers_bronze)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation,sequenceNum,_rescued_data,ingestion_date,source_path )
STORED AS
  SCD TYPE 2;


-- COMMAND ----------


