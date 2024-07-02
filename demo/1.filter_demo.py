# Databricks notebook source
races_df=spark.read.parquet('/mnt/formula1dlbiljana/processed/races')

# COMMAND ----------

df_joined=circuits_df.join(race_df, circuit_df.circuit_id==race_df.circuit_id,"inner")\
    .select()
