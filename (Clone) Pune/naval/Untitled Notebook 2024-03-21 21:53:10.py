# Databricks notebook source


# COMMAND ----------

# Import the required library for file system interaction
from pyspark.sql.functions import col

# Define the file path (replace with your actual path)
file_path = "/mnt/data/my_data.csv"

try:
    # Attempt to read the file using Spark DataFrame API
    data_df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Process the data (assuming no errors in the file itself)
    clean_df = data_df.withColumn("clean_column", col("original_column").cast("int"))
    clean_df.show(truncate=False)

except FileNotFoundError as e:
    # Handle case where the file doesn't exist
    print(f"Error: File '{file_path}' not found.")

    # Optionally, perform alternative actions (e.g., notify users, use default data)
    # ...


# COMMAND ----------


