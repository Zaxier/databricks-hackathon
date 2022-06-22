# Databricks notebook source
import os
def copy_repo_data_to_filestore(target_dir_name):
  dbutils.fs.cp(f'file:{os.getcwd()}/data', f'dbfs:/FileStore/hackathon/{target_dir_name}', recurse=True)
  
for team_name in ['rectifier', 'transformer', 'renewables', 'power', 'energy', 'high_voltage']:
  copy_repo_data_to_filestore(team_name)

# COMMAND ----------


