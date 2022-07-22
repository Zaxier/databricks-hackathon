# Databricks notebook source
import dlt

# COMMAND ----------

data_dir = '/xavier_demo/hackathon_data/demo_data'
display(dbutils.fs.ls(data_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set schemas

# COMMAND ----------

# MAGIC %run ./1_schema_def

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data

# COMMAND ----------

meter_readings = (spark.read
  .schema(meter_readings_schema)
  .option('header', True)
  .csv(f'{data_dir}/meters/meter_readings/')
)

meter_predictions = (spark.read
  .schema(meter_predictions_schema)
  .option('header', True)
  .csv(f'{data_dir}/meters/meter_predictions/')
)

temp_predictions = (spark.read
  .schema(temp_predictions_schema)
  .option('header', True)
  .csv(f'{data_dir}/temp/temp_predictions')
)

temp_readings = (spark.read
  .schema(temp_readings_schema)
  .option('header', True)
  .csv(f'{data_dir}/temp/temp_readings')
)

ref_stream = (spark.read
  .schema(ref_stream_schema)
  .option('header', True)
  .csv(f'{data_dir}/reference_data/ref_stream.csv')
)

ref_stream_sensor = (spark.read
  .schema(ref_stream_sensor_schema)
  .option('header', True)
  .csv(f'{data_dir}/reference_data/ref_stream_sensor.csv')
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Autoloader

# COMMAND ----------

meter_readings = (
  spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option('header', True)
    .schema(meter_readings_schema)
    .load(f'{data_dir}/meters/meter_readings/')
)

meter_predictions = (
  spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option('header', True)
    .schema(meter_predictions_schema)
    .load(f'{data_dir}/meters/meter_predictions/')
)

temp_predictions = (
  spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('header', True)
    .schema(temp_predictions_schema)
    .load(f'{data_dir}/temp/temp_predictions')
)

temp_readings = (
  spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('header', True)
    .schema(temp_readings_schema)
    .load(f'{data_dir}/temp/temp_readings')      
)

ref_stream = (
  spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('header', True)
    .schema(ref_stream_schema)
    .load(f'{data_dir}/reference_data/ref_stream.csv')
)

ref_stream_sensor = (
  spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('header', True)
    .schema(ref_stream_sensor_schema)
    .load(f'{data_dir}/reference_data/ref_stream_sensor.csv')
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to delta

# COMMAND ----------

dbname = 'xavier_hackathon_demo'

readings.write.format('delta').mode('overwrite').saveAsTable(f'{dbname}.meter_readings')
predictions.write.format('delta').mode('overwrite').saveAsTable(f'{dbname}.meter_predictions')
temp_predictions.write.format('delta').mode('overwrite').saveAsTable(f'{dbname}.temp_predictions')
temp_readings.write.format('delta').mode('overwrite').saveAsTable(f'{dbname}.temp_readings')
ref_stream.write.format('delta').mode('overwrite').saveAsTable(f'{dbname}.meter_readings')
ref_stream_sensor

# COMMAND ----------

# MAGIC %md
# MAGIC #### Work

# COMMAND ----------

display(ref_stream_sensor)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- CREATE TABLE xavier_hackathon_demo.meter_readings2
# MAGIC -- USING CSV LOCATION '/xavier_demo/hackathon_data/demo_data/meters/meter_readings/'

# COMMAND ----------

spark.sql('create database if not exists xavier_hackathon_demo')
df.write.format('delta').mode('overwrite').saveAsTable('xavier_hackathon_demo.meter_readings')

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from xavier_hackathon_demo.meter_readings

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema cascade

# COMMAND ----------


