-- Databricks notebook source
select schema_of_json('{"order_id":"000000005498","order_timestamp":"2022-04-12 17:08:00","customer_id":"C00617","quantity":1,"total":20,"books":[{"book_id":"B04","quantity":1,"subtotal":20}]}') as schema_orders

-- COMMAND ----------

SELECT v.*
FROM (
  SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
  FROM bronze
  WHERE topic = "orders")

-- COMMAND ----------


