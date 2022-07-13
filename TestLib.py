# Databricks notebook source
from libX import libx

# COMMAND ----------

print(libx.getText())

# COMMAND ----------

#help(libx)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd


@pandas_udf("string")
def testPandas(raw_text: pd.Series) -> pd.Series:
  #apply this function to every entry of the series, that have been given as an input
  return raw_text.apply(lambda x: x+libx.getText())


# COMMAND ----------

df = spark.read.csv('dbfs:/DemoData/stock_data/company_list/exchange=amex/')
display(df)

# COMMAND ----------

df=df.withColumn("added_libx",testPandas(df["_c0"]))
display(df)


# COMMAND ----------

print(df.head())
