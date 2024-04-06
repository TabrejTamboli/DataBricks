# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

def add_ingestion(input_df):
    output_df=input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------


