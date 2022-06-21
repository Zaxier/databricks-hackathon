# Databricks notebook source
from pyspark.sql.functions import to_timestamp, from_unixtime

readings = (
  readings.withColumn("timeEnd",to_timestamp("timeEnd"))
  .withColumn("timeRecorded", to_timestamp("timeRecorded"))
  .withColumn("metric_0", readings.metric_0.cast("float")))
