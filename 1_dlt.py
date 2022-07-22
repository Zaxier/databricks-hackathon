# Databricks notebook source
# MAGIC %run ./1_schema_def

# COMMAND ----------

import dlt

# COMMAND ----------


@dlt.table(
    table_properties = {'pipelines.autoOptimize.zOrderCols': 'time_end, stream_id', 'quality': 'bronze'},
    partition_cols=['month'])
def brz_meter_readings():
  return (
      spark.readStream.format('cloudFiles')
        .option('cloudFiles.format', 'csv')
        .option('header', True)
        .schema(meter_readings_schema)
        .load(f'{data_dir}/meters/meter_readings/')
  )
  
@dlt.table(
  table_properties = {'pipelines.autoOptimize.zOrderCols': 'time_end, stream_id', 'quality': 'bronze'})
def brz_meter_predictions():
  return (
    spark.readStream.format('cloudFiles')
      .option('cloudFiles.format', 'csv')
      .option('header', True)
      .schema(meter_predictions_schema)
      .load(f'{data_dir}/meters/meter_predictions/')
  )

@dlt.table(
  table_properties = {'pipelines.autoOptimize.zOrderCols': 'time_end, stream_id', 'quality': 'bronze'})
def brz_temp_predictions():
  return (
    spark.readStream.format('cloudFiles')
      .option('cloudFiles.format', 'csv')
      .option('header', True)
      .schema(temp_predictions_schema)
      .load(f'{data_dir}/temp/temp_predictions')
  )

@dlt.table(
  table_properties = {'pipelines.autoOptimize.zOrderCols': 'time_end, stream_id', 'quality': 'bronze'})
def brz_temp_readings():
  return (
    spark.readStream.format('cloudFiles')
      .option('cloudFiles.format', 'csv')
      .option('header', True)
      .schema(temp_readings_schema)
      .load(f'{data_dir}/temp/temp_readings')      
  )

@dlt.table(table_properties = {'quality': 'gold'})
def ref_stream():
  return (spark.read
    .schema(ref_stream_schema)
    .option('header', True)
    .csv(f'{data_dir}/reference_data/ref_stream.csv')
  )

@dlt.table(table_properties = {'quality': 'gold'})
def ref_stream_sensor():
  return (spark.read
    .schema(ref_stream_sensor_schema)
    .option('header', True)
    .csv(f'{data_dir}/reference_data/ref_stream_sensor.csv')
  )

