# Databricks notebook source
schema 
when
1. if you have large data 
2. do not have header


which
1.str/list
2. pyspark datatype

# COMMAND ----------

# MAGIC %md
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html

# COMMAND ----------

Special Spark Datatype
1. StructType
2. Array Type
3. Map Type


# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run /Workspace/Users/naval@cloudthat.net/Pune/day2/includes

# COMMAND ----------

{"driverId":1,"driverRef":"hamilton","number":44,"code":"HAM","name":{"forename":"Lewis","surname":"Hamilton"},"dob":"1985-01-07","nationality":"British","url":"http://en.wikipedia.org/wiki/Lewis_Hamilton"}

# COMMAND ----------

user_schema=StructType([StructField("driverId",IntegerType()),
                        StructField("driverRef",StringType()),
                        StructField("number",IntegerType()),
                        StructField("code",StringType()),
                        StructField("name",MapType(StringType(),StringType())),
                        StructField("dob",StringType()),
                        StructField("nationality",StringType()),
                        StructField("url",StringType())
                        ])

# COMMAND ----------

df_auto=spark.read.json(f"{input_raw_files}/drivers.json")

# COMMAND ----------

display(df_auto)

# COMMAND ----------

df_auto.withColumn("forename",col("name.forename"))\
.withColumn("surname",col("name.surname"))\
    .drop("name")\
.display()

# COMMAND ----------

df=spark.read.schema(user_schema).json(f"{input_raw_files}/drivers.json")

# COMMAND ----------

df.withColumn("forename",col("name.forename"))\
.withColumn("surname",col("name.surname"))\
    .drop("name")\
.display()

# COMMAND ----------

display(df)

# COMMAND ----------

schema_name=StructType([StructField("forename",StringType()),
                        StructField("surname",StringType())
])

# COMMAND ----------

user_schema_2=StructType([StructField("driverId",IntegerType()),
                        StructField("driverRef",StringType()),
                        StructField("number",IntegerType()),
                        StructField("code",StringType()),
                        StructField("name",schema_name),
                        StructField("dob",StringType()),
                        StructField("nationality",StringType()),
                        StructField("url",StringType())
                        ])

# COMMAND ----------

df=spark.read.schema(user_schema_2).json(f"{input_raw_files}/drivers.json")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/raw_json/

# COMMAND ----------

ddf=spark.read.json("dbfs:/mnt/cloudthats3/raw_json/array.json")

# COMMAND ----------

display(ddf)

# COMMAND ----------

schema_array=StructType([StructField("id",IntegerType()),
                         StructField("mobile",ArrayType(IntegerType())),
                         StructField("name",StringType())

])

# COMMAND ----------

df_schema=spark.read.schema(schema_array).json("dbfs:/mnt/cloudthats3/raw_json/array.json")

# COMMAND ----------

display(ddf)

# COMMAND ----------

df_schema.display()

# COMMAND ----------

df_schema.withColumn("mobile",explode("mobile")).display()

# COMMAND ----------


