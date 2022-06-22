# Databricks notebook source
def load_csv_to_spark_dataframe(path):
  return spark.read.format('csv').option("inferSchema" , "true").option('header', 'true').load(path)
    
def save_overwrite_delta_table(df, db_name, table_name, drop_table=False):
  if drop_table:
    spark.sql(f"drop table {db_name}.{table_name}")
  df.write.format('delta').option("mergeSchema", "true").mode('overwrite').saveAsTable(f'{db_name}.{table_name}')
  
def kickstarter_from_dbfs_to_spark_dfs(dbfs_path):
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

def kickstarter_from_dbfs_to_db(dbfs_path, team_name, drop_cascade=False):
  db_name = f'{team_name}_db'
  
  if drop_cascade:
    spark.sql(f"drop database {team_name} cascade;")
  
  spark.sql(f"create database if not exists {db_name}")
    
  file_list = ['metre_predictions.csv', 
               'metre_readings.csv', 
               'ref_stream_sensor.csv', 
               'ref_stream.csv', 
               'temp_readings.csv', 
               'temp_predictions.csv']

  
  for file in file_list:
    df = load_csv_to_spark_dataframe(f'{dbfs_path}/{file}')
    table_name = file.replace('.csv', '')
    print(f'Loading {file} to {team_name}_db.{table_name}')
    save_overwrite_delta_table(df, f'{team_name}_db', table_name)
    
  spark.sql(f'show tables in {db_name}').show()
  

# COMMAND ----------

# DBTITLE 1,Load datasets to your team's database
team_name = 'transformer'
dbfs_path = f'dbfs:/FileStore/hackathon/{team_name}'
drop_database = False

kickstarter_from_dbfs_to_db(dbfs_path, team_name, drop_database)

# COMMAND ----------

# DBTITLE 1,Load datasets to spark dataframes
team_name = 'transformer'
dbfs_path = f'dbfs:/FileStore/hackathon/{team_name}'
dataframe_dict = kickstarter_from_dbfs_to_spark_dfs()

metre_readings =    dataframe_dict['metre_readings']
metre_predictions = dataframe_dict['metre_predictions']
ref_stream =        dataframe_dict['ref_stream']
ref_stream_sensor = dataframe_dict['ref_stream_sensor']
temp_readings =     dataframe_dict['temp_readings']
temp_predictions =  dataframe_dict['temp_predictions']

# COMMAND ----------


