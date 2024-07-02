# Databricks notebook source
def mount_adls(storage_name,container_name):

    client_id=dbutils.secrets.get(scope='formula1-scope',key='formula1app-clientid')
    tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formulaapp-tenantid')
    secret_id=dbutils.secrets.get(scope='formula1-scope',key='formulaapp-secret')

    #define the configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id":client_id,
            "fs.azure.account.oauth2.client.secret": secret_id,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    #check if the container is already mounted
    if any(mounts.mountPoint==f"/mnt/{storage_name}/{container_name}" for mounts in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_name}/{container_name}")
    #mounting
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_name}/{container_name}",
        extra_configs = configs)

    display(dbutils.fs.mounts())






# COMMAND ----------

mount_adls('formula1dlbiljana','raw')

# COMMAND ----------

mount_adls('formula1dlbiljana','presentation')

# COMMAND ----------

mount_adls('formula1dlbiljana','processed')