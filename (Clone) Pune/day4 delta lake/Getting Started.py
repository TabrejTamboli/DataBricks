# Databricks notebook source
# MAGIC %md
# MAGIC ### Ways to create delta
# MAGIC 1. CTAS
# MAGIC 2. df 
# MAGIC 3. SQL/ Python
# MAGIC

# COMMAND ----------

# MAGIC   %fs ls dbfs:/mnt/cloudthats3/deltalake_pune/

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema ny_delta location 'dbfs:/mnt/cloudthats3/deltalake_pune/naval'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ny_delta.people10m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC )

# COMMAND ----------


