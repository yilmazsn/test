-- Databricks notebook source
-- MAGIC %python
-- MAGIC # There are sample datasets available for use..  copy the 
-- MAGIC # test for git
-- MAGIC display(dbutils.fs.ls("/databricks-datasets"))
-- MAGIC display(dbutils.fs.ls("/databricks-datasets/amazon"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # SETUP
-- MAGIC 
-- MAGIC DB = dbutils.widgets.get('database')
-- MAGIC PATH = dbutils.widgets.get('path')
-- MAGIC 
-- MAGIC dbutils.fs.rm('{}/deltademo'.format(PATH), True)
-- MAGIC dbutils.fs.mkdirs('{}/deltademo'.format(PATH))
-- MAGIC 
-- MAGIC dbutils.fs.cp('/databricks-datasets/amazon', '{}/deltademo/amazon'.format(PATH), True)
-- MAGIC dbutils.fs.cp('/databricks-datasets/iot', '{}/deltademo/iot'.format(PATH), True)
-- MAGIC 
-- MAGIC sql('DROP DATABASE IF EXISTS {} CASCADE'.format(DB))
-- MAGIC sql('CREATE DATABASE {}'.format(DB))
-- MAGIC sql('USE {}'.format(DB))
-- MAGIC 
-- MAGIC CP_PATH = "{}/checkpoints".format(PATH)
-- MAGIC 
-- MAGIC spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

-- COMMAND ----------

-- DBTITLE 1,Evolution of the Data Lake
-- MAGIC %md
-- MAGIC <img src=https://databricks.com/wp-content/uploads/2020/01/data-lakehouse.png width=800px>

-- COMMAND ----------

-- MAGIC %md # Part 1: Ingest
-- MAGIC ![delta0](files/dillon/delta0.png)

-- COMMAND ----------

-- DBTITLE 1,Example #1: Convert Parquet data on S3
CONVERT TO DELTA parquet.`/home/dillon.bostwick@databricks.com/deltademo/amazon/data20K`;

SELECT * FROM delta.`/home/dillon.bostwick@databricks.com/deltademo/amazon/data20K`

-- COMMAND ----------

CREATE TABLE TESTMIPI AS  SELECT * fROM csv.`/FileStore/tables/P1_DASW_Patching.csv`;
select * from TESTMIPI

-- COMMAND ----------

-- DBTITLE 1,Example #2: JSON files in S3
CREATE TABLE IOTEventsBronze
USING delta
AS SELECT * FROM json.`/home/dillon.bostwick@databricks.com/deltademo/iot/iot_devices.json`;

SELECT * FROM IOTEventsBronze;

-- COMMAND ----------

-- DBTITLE 1,Example #3: Use an ETL service
-- MAGIC %md 
-- MAGIC 
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/partner-integrations-and-sources.png" width="750" height="750">

-- COMMAND ----------

-- MAGIC %md # Part 2: Unified Data Lake
-- MAGIC ![delta1](files/dillon/delta1.png)

-- COMMAND ----------

-- DBTITLE 1,Example #1: Upsert into Silver table
CREATE TABLE IOTDevicesSilver (device_name STRING, latest_CO2 INT)
USING Delta;

MERGE INTO IOTDevicesSilver
USING IOTEventsBronze
ON IOTEventsBronze.device_name = IOTDevicesSilver.device_name
WHEN MATCHED
  THEN UPDATE SET latest_CO2 = c02_level
WHEN NOT MATCHED
  THEN INSERT (device_name, latest_CO2) VALUES (device_name, c02_level);

SELECT * FROM IOTDevicesSilver;

-- COMMAND ----------

-- DBTITLE 1,Example #2: Partitioning, Indexing, Caching
OPTIMIZE IOTDevicesSilver ZORDER BY device_name

-- COMMAND ----------

-- DBTITLE 1,Example #3: Consistent batch reads while streaming/writing
-- MAGIC %python
-- MAGIC spark.readStream.format('delta').table('IOTEventsBronze') \
-- MAGIC   .writeStream.format('delta') \
-- MAGIC   .option('checkpointLocation', '{}/IOTEvents'.format(CP_PATH)) \
-- MAGIC   .table('IOTEventsSilverRealtime')

-- COMMAND ----------

SELECT * FROM IOTDevicesSilver VERSION AS OF 0

-- COMMAND ----------

-- DBTITLE 1,Example #4: Replay historical data
DESCRIBE HISTORY IOTDevicesSilver

-- COMMAND ----------

-- DBTITLE 1,Vacuuming and GDPR
 VACUUM IOTEventsBronze RETAIN 720 HOURS

-- COMMAND ----------

/*SELECT * FROM IOTEventsSilverRealtime*/

-- COMMAND ----------

-- MAGIC %md # Part 3: Serve Analytics
-- MAGIC ![delta2](files/dillon/delta2.png)

-- COMMAND ----------

-- DBTITLE 1,Example #1: Build aggregates, Gold tables directly in Delta
CREATE TABLE IOTRollupGold
USING delta
AS SELECT
  device_name,
  c02_level,
  battery_level
FROM
  IOTEventsBronze
GROUP BY
  ROLLUP (device_name, c02_level, battery_level)
ORDER BY
  battery_level ASC,
  C02_level DESC;

-- COMMAND ----------

OPTIMIZE IOTRollupGold ZORDER BY battery_level

-- COMMAND ----------

-- DBTITLE 1,Example #2: Query Directly with SparkSQL
SELECT count(*) as Total_Devices FROM IOTRollupGold
WHERE battery_level = 0

-- COMMAND ----------

SELECT * FROM IOTRollupGold

-- COMMAND ----------

-- DBTITLE 1,Example #3: Serve BI from Gold table
-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="/files/dillon/delta-externals.png" width=1100 height=1100>
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC [BI Documentation](https://docs.databricks.com/integrations/bi/index.html);
-- MAGIC [External Reader Documentation](https://docs.databricks.com/delta/integrations.html)

-- COMMAND ----------

-- DBTITLE 1,Example #4: Machine Learning
-- MAGIC %python
-- MAGIC 
-- MAGIC import nltk
-- MAGIC from nltk.sentiment.vader import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC def sentimentNLTK(text):
-- MAGIC   nltk.download('vader_lexicon')
-- MAGIC   return SentimentIntensityAnalyzer().polarity_scores(text)
-- MAGIC 
-- MAGIC spark.udf.register('analyzeSentiment', sentimentNLTK, MapType(StringType(), DoubleType()))

-- COMMAND ----------

SELECT *, analyzeSentiment(review) AS sentiment
FROM delta.`/home/dillon.bostwick@databricks.com/deltademo/amazon/data20K`
ORDER BY sentiment.neg DESC

-- COMMAND ----------

-- MAGIC %md # Extras

-- COMMAND ----------

-- DBTITLE 1,Delta Implementation
-- MAGIC %fs ls /home/dillon.bostwick@databricks.com/deltademo/amazon/data20K

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/home/dillon.bostwick@databricks.com/deltademo/amazon/data20K/_delta_log/

-- COMMAND ----------

-- DBTITLE 1,Backwards Compatibility
VACUUM IOTEventsBronze RETAIN 0 HOURS