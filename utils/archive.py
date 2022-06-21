# Databricks notebook source
# MAGIC %run ./fetch_user_metadata

# COMMAND ----------

from pprint import pprint

config = {
  'db_name': db_name,
  'user_name': user_name,
  'user_email': user_email,
  'other...': 'xxx'
}

pprint(config)

def load_csv_to_spark_dataframe(path):
  return spark.read.format('csv').option('header', 'true').load(path)
  
def save_overwrite_delta_table(df, db_name, table_name, drop_table=False):
  if drop_table:
    spark.sql(f"drop table {db_name}.{table_name}")
  df.write.format('delta').option("mergeSchema", "true").mode('overwrite').saveAsTable(f'{db_name}.{table_name}')
  
def drop_database_cascade(db_name):
  spark.sql(f'drop {db_name} cascade;')
  


# COMMAND ----------

def kickstarter(config, drop_cascade=True):
  if drop_cascade:
    print(f"Dropping {config['db_name']} database")
    spark.sql(f"drop database {config['db_name']} cascade;")
  
  # Create database
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['db_name']};")

  # Load files to database
  file_list = ['metre_predictions.csv', 
               'metre_readings.csv', 
               'ref_stream_sensor.csv', 
               'ref_stream.csv', 
               'temp_readings.csv', 
               'temp_predictions.csv']
#   file_list = 'readings.csv'

  
  for file in file_list:
    df = load_csv_to_spark_dataframe(f'file:{os.getcwd()}/data/{file}')
    table_name = file.replace('.csv', '')
    print(f'Loading {file} to {db_name}.{table_name}')
    save_overwrite_delta_table(df, db_name, table_name)
    
  spark.sql(f'show tables in {db_name}').show()

# COMMAND ----------

kickstarter(config=config)
