-- Databricks notebook source
use catalog `cloudthat`; select * from `deepesh`.`superstore` limit 100;

-- COMMAND ----------

drop table cloudthat.deepesh.superstore

-- COMMAND ----------

select * from cloudthat.deepesh.superstore

-- COMMAND ----------

use catalog mmc_dev;
use bronze;

-- COMMAND ----------

CREATE OR REPLACE TABLE heartrate_device (device_id INT, mrn STRING, name STRING, time TIMESTAMP, heartrate DOUBLE);

INSERT INTO heartrate_device VALUES
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:01:58.000+0000", 54.0122153343),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:02:55.000+0000", 92.5136468131),
  (37, "65300842", "Samuel Hughes", "2020-02-01T00:08:58.000+0000", 52.1354807863),
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:16:51.000+0000", 54.6477014191),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:18:08.000+0000", 95.033344842);
  

-- COMMAND ----------

select * from mmc_dev.bronze.heartrate_device

-- COMMAND ----------

create or replace view heartrate_avg_view as 
(select mrn, name, avg(heartrate) as avg_heartrate from mmc_dev.bronze.heartrate_device group by all)

-- COMMAND ----------

GRANT ALL PRIVILEGES on catalog mmc_dev to `account users`

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
    ELSE mrn
  END AS mrn,
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
    ELSE name
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('dev') THEN 'REDACTED'
    ELSE mrn
  END AS mrn,
  CASE WHEN
    is_account_group_member('dev') THEN 'REDACTED'
    ELSE name
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)

-- COMMAND ----------

GRANT ALL PRIVILEGES on view mmc_dev.bronze.agg_heartrate to `dev`

-- COMMAND ----------

GRANT SELECT on view mmc_dev.bronze.agg_heartrate to `account users`

-- COMMAND ----------

select * from agg_heartrate

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('dev') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

GRANT ALL PRIVILEGES on view mmc_dev.bronze.agg_heartrate to `dev`

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mask(x STRING)
  RETURNS STRING
  RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2)
); 

-- COMMAND ----------

SELECT mask('sensitive data') AS data

-- COMMAND ----------

GRANT EXECUTE ON FUNCTION mask to `account users`

-- COMMAND ----------

select mask('Databricks')

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('dev') THEN mask(mrn)
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('dev') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

GRANT ALL PRIVILEGES on view mmc_dev.bronze.agg_heartrate to `dev`

-- COMMAND ----------

select * from agg_heartrate

-- COMMAND ----------

GRANT READ_METADATA on table mmc_dev.bronze.heartrate_device to `dev`

-- COMMAND ----------

create schema mmc_dev.naval

-- COMMAND ----------

sync schema mmc_dev.naval from hive_metastore.naval

-- COMMAND ----------


