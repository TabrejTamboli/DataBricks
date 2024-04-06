-- Databricks notebook source
-- MAGIC %python
-- MAGIC parquet files
-- MAGIC + 
-- MAGIC _delta_logs 
-- MAGIC 1.crc
-- MAGIC 2.json
-- MAGIC 3. checkpoint(parquet)

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/cloudthats3/deltalake_pune/naval/people10m/_delta_log/00000000000000000000.json

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/cloudthats3/deltalake_pune/naval/people10m/_delta_log/00000000000000000001.json

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/cloudthats3/deltalake_pune/naval/people10m/_delta_log/00000000000000000001.json%fs 
-- MAGIC head dbfs:/mnt/cloudthats3/deltalake_pune/naval/people10m/_delta_log/00000000000000000001.json

-- COMMAND ----------

 id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT

-- COMMAND ----------

INSERT INTO ny_delta.people10m VALUES(1,'naval','l','yemul',"Male", '2024-04-04T00:00:00','77799', 1000)

-- COMMAND ----------

select * from ny_delta.people10m

-- COMMAND ----------

INSERT INTO ny_delta.people10m VALUES(2,'a','l','z',"Male", '2024-04-04T00:00:00','77799', 1000),
                                      (3,'b','l','z',"Male", '2024-04-04T00:00:00','77799', 1000),
                                      (4,'c','l','z',"Male", '2024-04-04T00:00:00','77799', 1000)

-- COMMAND ----------

delete from ny_delta.people10m where id =1

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/cloudthats3/deltalake_pune/naval/people10m/_delta_log/00000000000000000003.json

-- COMMAND ----------

delete from ny_delta.people10m where id =2

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/cloudthats3/deltalake_pune/naval/people10m/_delta_log/00000000000000000004.json

-- COMMAND ----------

select * from ny_delta.people10m

-- COMMAND ----------

UPDATE ny_delta.people10m
SET salary=2000
where id= 3

-- COMMAND ----------

describe history ny_delta.people10m

-- COMMAND ----------

select * from ny_delta.people10m version as of 2

-- COMMAND ----------

select *,input_file_name() from ny_delta.people10m

-- COMMAND ----------

delete from ny_delta.people10m

-- COMMAND ----------

  select * from ny_delta.people10m

-- COMMAND ----------

restore table ny_delta.people10m to version as of 13

-- COMMAND ----------

select * from ny_delta.people10m

-- COMMAND ----------

optimize ny_delta.people10m
zorder by (id)

-- COMMAND ----------

describe history ny_delta.people10m

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/cloudthats3/deltalake_pune/naval/people10m/_delta_log/00000000000000000032.json

-- COMMAND ----------

vacuum ny_delta.people10m

-- COMMAND ----------

select * from ny_delta.people10m version as of 13

-- COMMAND ----------

vacuum ny_delta.people10m retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

SET spark.databricks.delta.vacuum.logging.enabled = True

-- COMMAND ----------

vacuum ny_delta.people10m retain 0 hours dry run

-- COMMAND ----------

vacuum ny_delta.people10m retain 0 hours

-- COMMAND ----------

select * from ny_delta.people10m version as of 13

-- COMMAND ----------

describe history ny_delta.people10m

-- COMMAND ----------


