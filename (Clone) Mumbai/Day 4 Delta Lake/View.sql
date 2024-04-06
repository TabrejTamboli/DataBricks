-- Databricks notebook source
Create view circuit_aust as  
(select * from hive_metastore.naval.circuit where country='Australia' )



-- COMMAND ----------

Create temp view circuit_spain as  
(select * from hive_metastore.naval.circuit where country='Spain' )

-- COMMAND ----------

select * from circuit_spain

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.table("circuit_spain")

-- COMMAND ----------

show views

-- COMMAND ----------

Create global temp view circuit_turkey as  
(select * from hive_metastore.naval.circuit where country='Turkey' )

-- COMMAND ----------

show views

-- COMMAND ----------

show views in global_temp

-- COMMAND ----------

select * from circuit_turkey

-- COMMAND ----------


select * from global_temp.circuit_turkey

-- COMMAND ----------


