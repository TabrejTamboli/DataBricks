-- Databricks notebook source
-- MAGIC %python
-- MAGIC https://docs.delta.io/latest/delta-batch.html#create-a-table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC way to create a Delta lake table
-- MAGIC 1. dataframe
-- MAGIC 2. sql
-- MAGIC 3. python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv("path")
-- MAGIC df.write.saveAsTable("tblname")

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/cloudthats3/

-- COMMAND ----------

CREATE TABLE naval.people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) LOCATION 'dbfs:/mnt/cloudthats3/delta_lake/naval/people10m'

-- COMMAND ----------

describe extended naval.people10m

-- COMMAND ----------

CREATE TABLE naval.people10m_managed (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
)

-- COMMAND ----------

describe extended naval.people10m_managed

-- COMMAND ----------

drop table naval.people10m

-- COMMAND ----------

select * from delta.`dbfs:/mnt/cloudthats3/delta_lake/naval/people10m`

-- COMMAND ----------


