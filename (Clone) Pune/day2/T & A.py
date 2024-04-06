# Databricks notebook source
# MAGIC %md
# MAGIC https://spark.apache.org/docs/latest/web-ui.html

# COMMAND ----------

df=spark.table("hive_metastore.amit.adobe_sample")

# COMMAND ----------

display(df)

# COMMAND ----------

df.filter("topping_id=5001 and batters_type ='Chocolate'").explain()

# COMMAND ----------

from pyspark.sql.functions import *
df.where(col("topping_id")==5001).display()

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy("topping_type").count().display()

# COMMAND ----------

df.groupBy("topping_type").count().explain()

# COMMAND ----------

df.orderBy("batters_type").display()

# COMMAND ----------

df.orderBy("batters_type",ascending=False).display()

# COMMAND ----------

df.orderBy("batters_type".desc()).display()

# COMMAND ----------

df.orderBy(col("batters_type").desc()).display()

# COMMAND ----------

df.orderBy(desc("batters_type")).display()

# COMMAND ----------

df.orderBy(desc("batters_type"),desc("topping_id")).display()

# COMMAND ----------


