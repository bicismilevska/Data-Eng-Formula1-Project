-- Databricks notebook source
DROP DATABASE f1_presentation

-- COMMAND ----------

SHOW TABLES In f1_presentation

-- COMMAND ----------

drop table f1_presentation.race_results

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dlbiljana/presentation"