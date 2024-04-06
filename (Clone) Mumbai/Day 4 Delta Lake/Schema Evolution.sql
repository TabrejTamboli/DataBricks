-- Databricks notebook source
-- MAGIC %python
-- MAGIC data1=([(1,'a',30),(2,'b',27)])
-- MAGIC schema1="id int, name string, age int"
-- MAGIC df1=spark.createDataFrame(data1,schema1)
-- MAGIC df1.write.saveAsTable("naval.emp_demo")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data2=([(3,'c',30),(4,'d',27)])
-- MAGIC schema2="id int, name string, age int"
-- MAGIC df2=spark.createDataFrame(data2,schema2)
-- MAGIC df2.write.mode("append").saveAsTable("naval.emp_demo")

-- COMMAND ----------

select * from naval.emp_demo

-- COMMAND ----------

describe history naval.emp_demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data3=([(5,'e',30,'Sales'),(6,'f',27,"IT")])
-- MAGIC schema3="id int, name string, age int, dept string"
-- MAGIC df3=spark.createDataFrame(data3,schema3)
-- MAGIC df3.write.mode("append").saveAsTable("naval.emp_demo")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df3.write.mode("append").option("mergeSchema",True).saveAsTable("naval.emp_demo")

-- COMMAND ----------

select * from naval.emp_demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df4=spark.read.table("naval.emp_demo")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df5=df4.dropDuplicates(["id"])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df5.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df5.write.mode("overwrite").option("mergeSchema",True).saveAsTable("naval.emp_demo")

-- COMMAND ----------


