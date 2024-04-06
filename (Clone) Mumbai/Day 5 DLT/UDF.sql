-- Databricks notebook source
CREATE OR REPLACE FUNCTION naval.sale_announcement(item_name STRING, item_price INT)
RETURNS STRING
RETURN concat("The ", item_name, " is on sale for $", round(item_price * 0.8, 0));

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED sale_announcement

-- COMMAND ----------


