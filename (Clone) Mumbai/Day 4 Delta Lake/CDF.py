# Databricks notebook source
https://docs.databricks.com/en/delta/delta-change-data-feed.html#enable-change-data-feed

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table naval.silverTable

# COMMAND ----------

countries = [("USA", 10000, 20000), ("India", 1000, 1500), ("UK", 7000, 10000), ("Canada", 500, 700) ]
columns = ["Country","NumVaccinated","AvailableDoses"]
spark.createDataFrame(data=countries, schema = columns).write.mode("overwrite").saveAsTable("naval.silverTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.silverTable

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *
spark.read.table("naval.silverTable").withColumn("VaccinationRate",col("NumVaccinated")/col("AvailableDoses")).drop("NumVaccinated","AvailableDoses").write.mode("overwrite").saveAsTable("naval.goldTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.goldTable

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE naval.silvertable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# Insert new records
new_countries = [("Australia", 100, 3000)]
columns = ["Country","NumVaccinated","AvailableDoses"]
spark.createDataFrame(data=new_countries, schema = columns).write.format("delta").mode("append").saveAsTable("naval.silverTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.silverTable

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update a record
# MAGIC UPDATE naval.silverTable SET NumVaccinated = '11000' WHERE Country = 'USA'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete a record
# MAGIC DELETE from naval.silverTable WHERE Country = 'UK'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history naval.silverTable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('naval.silverTable',2, 4) 

# COMMAND ----------

changes_df = spark.read.option("readChangeData", True).option("startingVersion", 2).table('silverTable')
display(changes_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC (SELECT *, rank() over (partition by Country order by _commit_version desc) as rank
# MAGIC           FROM table_changes('silverTable', 2,4)
# MAGIC           WHERE _change_type !='update_preimage')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Collect only the latest version for each country
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silverTable_latest_version as
# MAGIC SELECT * 
# MAGIC     FROM 
# MAGIC          (SELECT *, rank() over (partition by Country order by _commit_version desc) as rank
# MAGIC           FROM table_changes('naval.silverTable', 2,4)
# MAGIC           WHERE _change_type !='update_preimage')
# MAGIC     WHERE rank=1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge the changes to gold
# MAGIC MERGE INTO naval.goldTable t USING silverTable_latest_version s ON s.Country = t.Country
# MAGIC         WHEN MATCHED AND s._change_type='update_postimage' THEN UPDATE SET VaccinationRate = s.NumVaccinated/s.AvailableDoses
# MAGIC         WHEN Matched and s._change_type='delete' then delete
# MAGIC         WHEN NOT MATCHED THEN INSERT (Country, VaccinationRate) VALUES (s.Country, s.NumVaccinated/s.AvailableDoses)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.goldTable

# COMMAND ----------


