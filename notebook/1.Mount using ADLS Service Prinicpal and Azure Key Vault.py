# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

clientID = dbutils.secrets.get(scope='azurekeyvault_formula1dl',key='clientID')

# COMMAND ----------

tenantID = dbutils.secrets.get(scope='azurekeyvault_formula1dl',key='tenantID')

# COMMAND ----------

secret = dbutils.secrets.get(scope='azurekeyvault_formula1dl',key='secretvalue')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl9119.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl9119.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl9119.dfs.core.windows.net", f"{clientID}")
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl9119.dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl9119.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantID}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls('abfss://raw@formula1dl9119.dfs.core.windows.net'))

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------



# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{clientID}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope='azurekeyvault_formula1dl',key='secretvalue'),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://raw@formula1dl9119.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

circuits_df = spark.read.csv('/mnt/formula1dl/raw/circuits.csv')

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md 
# MAGIC Mount Silver and Gold Containers too

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{clientID}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope='azurekeyvault_formula1dl',key='secretvalue'),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://silver@formula1dl9119.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/silver",
  extra_configs = configs)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{clientID}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope='azurekeyvault_formula1dl',key='secretvalue'),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://gold@formula1dl9119.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/gold",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Incremental Load Containers too

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{clientID}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope='azurekeyvault_formula1dl',key='secretvalue'),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://rawincremental@formula1dl9119.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/rawincremental",
  extra_configs = configs)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{clientID}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope='azurekeyvault_formula1dl',key='secretvalue'),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://silverincremental@formula1dl9119.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/silverincremental",
  extra_configs = configs)

# COMMAND ----------


