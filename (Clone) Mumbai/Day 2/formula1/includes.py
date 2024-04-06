# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

input_path="dbfs:/mnt/cloudthats3/formula1_raw/"
output_path="dbfs:/mnt/cloudthats3/formula1_processed_parquet/"
input_stream_path="dbfs:/mnt/cloudthats3/stream/input/csv_raw/emp"
