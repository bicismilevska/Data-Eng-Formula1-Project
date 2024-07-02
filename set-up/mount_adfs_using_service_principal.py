# Databricks notebook source
client_id=dbutils.secrets.get(scope='formula1-scope',key='formula1app-clientid')
tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formulaapp-tenantid')
secret_id=dbutils.secrets.get(scope='formula1-scope',key='formulaapp-secret')

# COMMAND ----------



# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id":client_id,
          "fs.azure.account.oauth2.client.secret": secret_id,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlbiljana.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlbiljana/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlbiljana/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlbiljana/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())