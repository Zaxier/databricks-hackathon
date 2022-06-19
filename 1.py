# Databricks notebook source
/dbfs/FileStore/xavier_armitage/sapn_raw



# COMMAND ----------

df1 = spark.read.format("csv").option("header", True).load("/FileStore/xavier_armitage/sapn_raw/readings-1.csv")
# df2 = spark.read.format("csv").option("header", True).load("/FileStore/xavier_armitage/sapn_raw/readings.csv")

# union = df1.union(df2)
print(df1.count())
# print(df2.count())

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/xavier_armitage/sapn_raw/sites.csv

# COMMAND ----------

df1.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv("/FileStore/xavier_armitage/sapn_raw/readings-tmp.csv")

# COMMAND ----------

df3 = spark.read.format("csv").option("header", True).load("/FileStore/xavier_armitage/sapn_raw/readings-tmp.csv")

# COMMAND ----------

df3.show()

# COMMAND ----------


