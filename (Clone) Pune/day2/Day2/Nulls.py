# Databricks notebook source
# MAGIC %md
# MAGIC - Drop
# MAGIC - Fill 
# MAGIC - Replace

# COMMAND ----------

schema=["name", "subject", "Marks", "Attendance"]

student_data=[("John","Math", 90, 80),("Michael", "Science", 70, None), ("David", "History", 50,40), ("Kelvin", "Computer", 40,None ),("Paul", "GEO", None, None), (None,None,  None, None),("John","Math", 90, 80),("John","Math", 90, 80),(None,None,  None, None),(None,None,  None, None),(None,None,  None, None) ]

df=spark.createDataFrame(data=student_data, schema=schema)
display(df)
df.printSchema()

# COMMAND ----------

df1=df.dropDuplicates(["name"])

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.dropna("all",subset="Marks").display()

# COMMAND ----------

df1.fillna("unknow",subset="name").display()

# COMMAND ----------

df.fillna({"name":"unknown","subject":"CS","Marks":50}).display()

# COMMAND ----------

df.replace("John","Romal").display()

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.dropna("all"))

# COMMAND ----------

display(df.dropna("any"))

# COMMAND ----------

display(df.na.drop(subset="Attendance"))

# COMMAND ----------

display(df.na.drop(thresh=4))

# COMMAND ----------

display(df)

# COMMAND ----------

df.dropna(how="all",subset=["Marks"]).show()

# COMMAND ----------

help(df.fillna)

# COMMAND ----------

display(df.fillna("unknown"))

# COMMAND ----------

display(df.fillna({"name":"unknow","subject":"EVS","Marks":40,"Attendance":35})).show()

# COMMAND ----------

display(df)
df.printSchema()

# COMMAND ----------

df.fillna({"name": "unknow", "subject":"all","Marks":35, "Attendance": 0}).show()

# COMMAND ----------

df.na.fill("unknow").show()

# COMMAND ----------

df.fillna(50).show()

# COMMAND ----------

df.fillna("unknown").show()

# COMMAND ----------

df.fillna(True).show()

# COMMAND ----------

df.fillna({'name':"unknow", 'subject':"cs", 'marks':35}).show()

# COMMAND ----------

df.fillna(value=0,subset=["Marks"]).show()

# COMMAND ----------

df.fillna(value="unknown",subset=["name"]).show()

# COMMAND ----------

display(df.na.fill("unknow"))

# COMMAND ----------

#display(df.filter(df.Marks.isNull()))
#display(df.filter("Marks is Null"))

from pyspark.sql.functions import col
display(df.filter(col("Marks").isNull()))

# COMMAND ----------

display(df.filter((df.Marks.isNotNull())& (df.Attendance.isNotNull())))

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

#display(df.na.drop())
display(df.dropna("any"))

# COMMAND ----------

df.dropna("all").show()

# COMMAND ----------

display(df.dropna(subset=["Marks"]))

# COMMAND ----------

display(df.na.fill(50))

# COMMAND ----------

display(df.na.fill(50, subset=["Attendance"]))

# COMMAND ----------

display(df.na.fill("unknow", subset=["name"]))

# COMMAND ----------

display(df.na.fill({'name':'unknow', 'subject': 'all', 'Marks':50}))
