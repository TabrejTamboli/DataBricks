# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC create temp view bronze_view as (SELECT cast(key AS STRING), cast(value AS STRING), topic
# MAGIC FROM bronze)

# COMMAND ----------

# MAGIC %sql
# MAGIC select MAX(len(value)) from bronze_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select value:order_id, value:order_timestamp from bronze_view where topic="orders"

# COMMAND ----------

# MAGIC %sql
# MAGIC select schema_of_json('{"order_id":"000000003491","order_timestamp":"2021-11-12 09:10:00","customer_id":"C00002","quantity":3,"total":99,"books":[{"book_id":"B02","quantity":1,"subtotal":28},{"book_id":"B06","quantity":1,"subtotal":22},{"book_id":"B01","quantity":1,"subtotal":49}]}') as schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_view where topic="orders"

# COMMAND ----------

# MAGIC %sql
# MAGIC select schema_of_json('{"customer_id":"C00014","email":"raeryaz@photobucket.com","first_name":"Roshelle","last_name":"Aery","gender":"Female","street":"80 Sunnyside Parkway","city":"Beiqi","country_code":"CN","row_status":"insert","row_time":"2021-11-14T14:06:00.000Z"}') as schema_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select schema_of_json('{"order_id":"000000005498","order_timestamp":"2022-04-12 17:08:00","customer_id":"C00617","quantity":1,"total":20,"books":[{"book_id":"B04","quantity":1,"subtotal":20}]}') as schema_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

(spark.readStream
      .table("bronze")
      .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze_tmp
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC     FROM bronze_tmp
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

query = (spark.table("orders_silver_tmp")
               .writeStream
               .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
               .trigger(availableNow=True)
               .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver

# COMMAND ----------


