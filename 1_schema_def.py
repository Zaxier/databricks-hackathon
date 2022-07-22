# Databricks notebook source
from pyspark.sql.types import *

meter_readings_schema = StructType([
  StructField('stream_id', StringType(), True),
  StructField('time_end', TimestampType(), True),
  StructField('time_recorded', TimestampType(), True),
  StructField('reading_type', StringType(), True),
  StructField('phenomena_1', DoubleType(), True),
  StructField('phenomena_2', DoubleType(), True),
  StructField('phenomena_3', DoubleType(), True),
  StructField('phenomena_4', DoubleType(), True),
])

meter_predictions_schema = meter_readings_schema

temp_predictions_schema = StructType([
  StructField('stream_id', StringType(), True),
  StructField('time_end', TimestampType(), True),
  StructField('time_recorded', TimestampType(), True),
  StructField('phenomena_id', StringType(), True),
  StructField('temp_avg', DoubleType(), True),
])

temp_readings_schema = StructType([
  StructField('stream_id', StringType(), True),
  StructField('time_end', TimestampType(), True),
  StructField('time_recorded', TimestampType(), True),
  StructField('phenomena_id', StringType(), True),
  StructField('temp_max', DoubleType(), True),
  StructField('temp_avg', DoubleType(), True),
  StructField('temp_min', DoubleType(), True),
])

ref_stream_schema = StructType([
  StructField('stream_id', StringType(), True),
  StructField('stream_name', StringType(), True),
  StructField('latitude', DoubleType(), True),
  StructField('longitude', DoubleType(), True),
  StructField('location', StringType(), True)
])

ref_stream_sensor_schema = StructType([
  StructField('id', StringType(), True),
  StructField('stream_id', StringType(), True),
  StructField('sensor_id', StringType(), True),
  StructField('start', TimestampType(), True),
  StructField('end', TimestampType(), True)
])
