# Databricks notebook source
dbutils.secrets.help()
client_id=dbutils.secrets.get(scope='formula1-scope',key='formula1app-clientid')
tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formulaapp-tenantid')
secret_id=dbutils.secrets.get(scope='formula1-scope',key='formulaapp-secret')

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.formula1dlbiljana.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlbiljana.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlbiljana.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlbiljana.dfs.core.windows.net", secret_id)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlbiljana.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlbiljana.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlbiljana.dfs.core.windows.net/circuits.csv"))