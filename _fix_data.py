# Databricks notebook source
import hashlib
from pyspark.sql.types import *
from numpy.random import uniform

def encrypt_value(val):
  sha_value = hashlib.sha256(val.encode()).hexdigest()
  return sha_value

def encrypt_value_short(val):
  sha_value = hashlib.sha256(val.encode()).hexdigest()
  return sha_value[:6]

def add_noise(val):
  return val + uniform(-15,15)

from pyspark.sql.functions import udf

hash_udf = udf(encrypt_value, StringType())
hash_short_udf = udf(encrypt_value_short, StringType())
add_noise_udf = udf(add_noise, FloatType())

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Readings

# COMMAND ----------

readings_raw = spark.read.format("csv").option("header", True).load("/FileStore/xavier_armitage/sapn_raw/readings_final.csv")
readings_raw.createOrReplaceTempView("readings_raw")

readings_standard = spark.sql("""select
  phenomenaStreamId,
  timeEnd,
  timeRecorded,
  sensorId,
  phenomenaId,
  maximum as metric_0_max,
  CASE
    WHEN maximum = 'null' then NULL
    ELSE average7
  END as metric_0_avg,
  minimum as metric_0_min,
  metric_rh as metric_1,
  metric_temp as metric_2,
  eval_wetbulb as metric_3,
  CASE
    WHEN metric_rh = 'null' then NULL
    ELSE average12
  END as metric_4
from
  readings_raw
""")

readings_hashed = (
  readings_standard.
  withColumn("phenomenaStreamId", hash_udf("phenomenaStreamId")).
  withColumn("sensorId", hash_udf("sensorId")).
  withColumn("phenomenaId", hash_udf("phenomenaId"))
)

readings_hashed.createOrReplaceTempView("readings_hashed")

# TODO : write somewhere

# COMMAND ----------



# COMMAND ----------

# PhenomenaStreams
ps = spark.read.format("csv").option("header", "true").load("/FileStore/xavier_armitage/sapn_raw/phenomena_stream.csv").withColumnRenamed("id", "phenomenaStreamId")
ps.createOrReplaceTempView('ps')
ps = spark.sql("""
  select 
    CASE WHEN phenomenaStreamId = '014a9cd1-cc06-3457-a41c-7908392ec305' THEN 'bb8b587c-a36a-3387-9ca6-639ea1b2aa03'
     WHEN phenomenaStreamId = '0186e668-de08-4d9b-ae29-0d06886f2175' THEN 'e1488468-c37d-370e-98bb-24fabe2805e5'
     WHEN phenomenaStreamId = '022ce43a-12d5-3c8a-8ef0-daaa81994302' THEN '44e6c1cd-a7db-3941-bd2e-4581129d442c'
     WHEN phenomenaStreamId = '01f67dd8-3a6f-3763-8c27-f44d132fef1a' THEN '0c880190-b574-3bdd-a099-471af1fc0c4c'
     WHEN phenomenaStreamId = '0191f12c-203c-33d7-a2ce-257f91bf9ba4' THEN 'dd964e94-fbc5-3805-b20c-bb0cad7e44eb'
     WHEN phenomenaStreamId = '028a620b-b512-4fa4-9846-3c80f4fe1530' THEN '68fd637d-c8b1-3caf-9f9f-af832bb1952e'
     WHEN phenomenaStreamId = '0053325a-b848-49fd-84cc-b82eec69a117' THEN '7b3facba-a978-3db8-903c-f08e88f694a4'
     WHEN phenomenaStreamId = '006f1240-c487-4e3e-843c-41683815784f' THEN '084ef9be-bdc5-320e-8adc-2fe4eb23254a'
     WHEN phenomenaStreamId = '027d01bd-8259-3b38-af07-05bd86cb5631' THEN '032cd192-a586-41b3-8b91-60d01d708a1a'
    ELSE phenomenaStreamId
    END as phenomenaStreamId,
    name,
    phenomenaId,
    companyId,
    cast(latitude as float) latitude,
    cast(longitude as float) longitude,
    timeZone,
    showPrediction,
    height
  from 
  ps
  """)
ps = (ps.withColumn("phenomenaStreamId", hash_udf("phenomenaStreamId"))
      .withColumn("name", hash_short_udf("name"))
      .withColumn("phenomenaId", hash_udf("phenomenaId"))
      .withColumn("companyId", hash_short_udf("companyId"))
      .withColumn("latitude", add_noise_udf("latitude"))
      .withColumn("longitude", add_noise_udf("longitude"))
     )

ps.createOrReplaceTempView('ps')
ps = spark.sql("""select * from ps where phenomenaStreamId in (select distinct(phenomenaStreamId) from readings_hashed)""")
ps.createOrReplaceTempView('ps')

# psg.createOrReplaceTempView("psg")
# psg = spark.sql("""select * from psg where phenomenaStreamId in (select distinct(phenomenaStreamId) from readings_hashed)""")


psg_ps = spark.read.format("csv").option("header", "true").load("/FileStore/xavier_armitage/sapn_raw/phenomena_stream_group_phenomena_stream.csv")
psg_ps = (
  psg_ps.withColumnRenamed("id", "phenomenaStreamGroupPhenomenaStream")
  .withColumn("phenomenaStreamGroupPhenomenaStream", hash_udf("phenomenaStreamGroupPhenomenaStream"))
  .withColumn("phenomenaStreamId", hash_udf("phenomenaStreamId"))
  .withColumn("phenomenaStreamGroupId", hash_udf("phenomenaStreamGroupId"))
)
psg_ps.createOrReplaceTempView("psg_ps")
psg_ps = spark.sql("""select * from psg_ps where phenomenaStreamId in (select distinct(phenomenaStreamId) from readings_hashed)""")


psg = spark.read.format("csv").option("header", "true").load("/FileStore/xavier_armitage/sapn_raw/phenomena_stream_group.csv")
psg = (psg.withColumnRenamed("id", "phenomenaStreamGroupId")
       .withColumn("phenomenaStreamGroupId", hash_udf("phenomenaStreamGroupId"))
       .withColumn("name", hash_short_udf("name"))
       .withColumn("companyId", hash_short_udf("companyId"))
      )


readings_hashed_enriched = readings_hashed.join(ps, ['phenomenaStreamId'])

# COMMAND ----------

ps.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC * from ps --where phenomenaId in ('b958ce8b871ab36b3ceee0e0f012b6182962b2db7926be0a670149f9fbd00b84', '69a992ae04b351d178a731d57ea821ccbb1e86d97f0e0b2c5ca7c8c402968c91')

# COMMAND ----------

readings_hashed_enriched.count()

# COMMAND ----------

display(readings_hashed_enriched.select('phenomenaStreamId').distinct())

# COMMAND ----------

display(df2_ps)

# COMMAND ----------

display(psg_ps.where("phenomenaStreamId = "))

# COMMAND ----------

display(df3)

# COMMAND ----------

joined = df3.join(psg_ps, ['phenomenaStreamId'])

# COMMAND ----------

joined.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Archive

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/FileStore/xavier_armitage/sapn_raw/readings_final-1.csv

# COMMAND ----------


