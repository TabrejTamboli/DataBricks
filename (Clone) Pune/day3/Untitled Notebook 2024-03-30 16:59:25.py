# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/institute_data/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/cloudthats3/institute_data/registration/",header=True,multiLine=True,inferSchema=True)

# COMMAND ----------

df.write.option("delta.columnMapping.mode","name").saveAsTable("naval.reg")

# COMMAND ----------



# COMMAND ----------

df.columns

# COMMAND ----------

new_columns=['registration_id',
 'timestamp',
 'email_address',
 'email_id',
 'full_name',
 'contact_no',
 'state&country',
 'degree',
 'occupation',
 'job_title',
 'organization',
 'years_of_exp',
 'skills',
 'course_id']

# COMMAND ----------

df_new=df.toDF(*new_columns)

# COMMAND ----------

display(df_new)

# COMMAND ----------

df1.write.save

# COMMAND ----------

df_feedback=spark.read.csv("dbfs:/mnt/cloudthats3/institute_data/feedback/",header=True,inferSchema=True)

# COMMAND ----------

display(df_feedback)

# COMMAND ----------


