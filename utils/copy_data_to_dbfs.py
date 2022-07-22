# Databricks notebook source
import os
def copy_repo_data_to_filestore(target_dir_name):
#   dbutils.fs.cp(f'file:{os.getcwd()}/data', f'dbfs:/FileStore/xavier_armitage/hackathon_data/{target_dir_name}', recurse=True)
  dbutils.fs.cp(f'file:{os.getcwd()}/data', f'dbfs:/xavier_demo/hackathon_data/{target_dir_name}', recurse=True)

def setup_team_names():
  for team_name in ['rectifier', 'transformer', 'renewables', 'power', 'energy', 'high_voltage']:
    copy_repo_data_to_filestore(team_name)

# COMMAND ----------


