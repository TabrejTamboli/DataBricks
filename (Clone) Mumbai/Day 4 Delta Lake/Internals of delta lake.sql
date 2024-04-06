-- Databricks notebook source
parquet files
+
_delta_log
1. crc
2. json
3. parquet check point

-- COMMAND ----------

CREATE TABLE naval.people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) LOCATION 'dbfs:/mnt/cloudthats3/delta_lake/naval/people10m'

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/cloudthats3/delta_lake/naval/people10m/_delta_log/00000000000000000000.json

-- COMMAND ----------

describe history naval.people10m

-- COMMAND ----------

{"commitInfo":{"timestamp":1710997563140,"userId":"4940231918153097","userName":"naval@cloudthat.net","operation":"CREATE TABLE","operationParameters":{"partitionBy":"[]","description":null,"isManaged":"false","properties":"{}","statsOnLoad":false},"notebook":{"notebookId":"2518834888644600"},"clusterId":"0315-062356-7zq6rkld","isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{},"tags":{"restoresDeletedRows":"false"},"engineInfo":"Databricks-Runtime/13.3.x-scala2.12","txnId":"276bce97-882c-42e8-ae9c-730465eb6b76"}}
{"metaData":{"id":"4de40c91-b37b-499c-b5e2-de3a52139c8b","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"firstName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"middleName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gender\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthDate\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ssn\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"salary\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1710997562863}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}


-- COMMAND ----------

INSERT INTO naval.people10m VALUES(1,'a','n','z',"MALE",'2024-03-21T00:00:0',"1234",1000)

-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------

INSERT INTO naval.people10m VALUES(2,'b','n','z',"MALE",'2024-03-21T00:00:0',"1234",1000),
                                  (3,'c','n','z',"MALE",'2024-03-21T00:00:0',"1234",1000)

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/cloudthats3/delta_lake/naval/people10m/_delta_log/00000000000000000001.json

-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------

delete from naval.people10m where id =1 

-- COMMAND ----------

delete from naval.people10m where id =2

-- COMMAND ----------

describe history naval.people10m

-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------

INSERT INTO naval.people10m VALUES(4,'b','n','z',"MALE",'2024-03-21T00:00:0',"1234",1000),
                                  (5,'c','n','z',"MALE",'2024-03-21T00:00:0',"1234",1000),
                                  (6,'c','n','z',"MALE",'2024-03-21T00:00:0',"1234",1000),
                                  (7,'c','n','z',"MALE",'2024-03-21T00:00:0',"1234",1000),
                                  (8,'c','n','z',"MALE",'2024-03-21T00:00:0',"1234",1000),
                                  (9,'c','n','z',"MALE",'2024-03-21T00:00:0',"1234",1000)

-- COMMAND ----------

UPDATE naval.people10m
SET salary = 2000
where id = 5

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/cloudthats3/delta_lake/naval/people10m/_delta_log/00000000000000000008.json

-- COMMAND ----------

describe history naval.people10m

-- COMMAND ----------

select * from naval.people10m version as of 2

-- COMMAND ----------

select * from naval.people10m timestamp as of '2024-03-21T05:17:12Z'

-- COMMAND ----------

delete from naval.people10m

-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------

describe history naval.people10m

-- COMMAND ----------

RESTORE TABLE naval.people10m to version as of 8

-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------



-- COMMAND ----------

select * from delta.`dbfs:/mnt/cloudthats3/delta_lake/naval/people10m`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.parquet("dbfs:/mnt/cloudthats3/delta_lake/naval/people10m/_delta_log/00000000000000000010.checkpoint.parquet")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

optimize naval.people10m
zorder by (id)

-- COMMAND ----------

describe history naval.people10m

-- COMMAND ----------

vacuum naval.people10m

-- COMMAND ----------

select * from naval.people10m version as of 2

-- COMMAND ----------



-- COMMAND ----------

vacuum naval.people10m retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum naval.people10m retain 0 hours dry run

-- COMMAND ----------

vacuum naval.people10m retain 0 hours

-- COMMAND ----------

vacuum naval.people10m retain 0 hours dry run

-- COMMAND ----------

describe history naval.people10m

-- COMMAND ----------

describe extended naval.people10m

-- COMMAND ----------

select * from naval.people10m version as of 4

-- COMMAND ----------

refresh table naval.people10m

-- COMMAND ----------

create table naval.emp(id int, name string);
insert into naval.emp(1,'a'),(2,'b'),(3,'c'),(4,d);
delete from naval.emp where id=1 


-- COMMAND ----------

https://docs.databricks.com/en/delta/vacuum.html

-- COMMAND ----------

SET spark.databricks.delta.vacuum.logging.enabled=True

-- COMMAND ----------

vacuum naval.people10m retain 0 hours

-- COMMAND ----------

describe history naval.people10m

-- COMMAND ----------

select * from naval.people10m version as of 4

-- COMMAND ----------

create table naval.emp(id int, name string);
insert into naval.emp(1,'a'),(2,'b'),(3,'c'),(4,d);
delete from naval.emp where id=1 
