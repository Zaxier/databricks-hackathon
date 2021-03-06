# Databricks notebook source
def load_csv_to_spark_dataframe(path):
  return spark.read.format('csv').option("inferSchema" , "true").option('header', 'true').load(path)
  
def save_overwrite_delta_table(df, db_name, table_name, drop_table=False):
  if drop_table:
    spark.sql(f"drop table {db_name}.{table_name}")
  df.write.format('delta').option("mergeSchema", "true").mode('overwrite').saveAsTable(f'{db_name}.{table_name}')
  
def kickstarter_from_repo(config, drop_cascade=True):
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

  
  for file in file_list:
    df = load_csv_to_spark_dataframe(f'file:{os.getcwd()}/data/{file}')
    table_name = file.replace('.csv', '')
    print(f'Loading {file} to {db_name}.{table_name}')
    save_overwrite_delta_table(df, db_name, table_name)
    
  spark.sql(f'show tables in {db_name}').show()
  
    
def kickstarter_from_dbfs(dbfs_path):
  
  # Load files to database
  file_list = ['metre_predictions.csv', 
               'metre_readings.csv', 
               'ref_stream_sensor.csv', 
               'ref_stream.csv', 
               'temp_readings.csv', 
               'temp_predictions.csv']

  dataframe_dict = {}
  for file in file_list:
    df = load_csv_to_spark_dataframe(f'{dbfs_path}/{file}')
    dataframe_dict[file.replace('.csv', '')] = df
  return dataframe_dict
  

# COMMAND ----------

team_name = 'transformer'
dataframe_dict = kickstarter_from_dbfs(f'dbfs:/FileStore/hackathon/{team_name}')

metre_readings =    dataframe_dict['metre_readings']
metre_predictions = dataframe_dict['metre_predictions']
ref_stream =        dataframe_dict['ref_stream']
ref_stream_sensor = dataframe_dict['ref_stream_sensor']
temp_readings =     dataframe_dict['temp_readings']
temp_predictions =  dataframe_dict['temp_predictions']

# COMMAND ----------


