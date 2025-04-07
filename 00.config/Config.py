# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS toll_reconciliation_tool;
# MAGIC USE CATALOG toll_reconciliation_tool;

# COMMAND ----------

%pip install pydrive

# COMMAND ----------

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.clientsecrets import InvalidClientSecretsError

# Update the path to your client secrets file
client_secrets_file = '/Workspace/Users/renato.maciel.silva@gmail.com/toll-reconciliation-tool/00.config/client_secret.json'

# Authenticate with the client secrets file
gauth = GoogleAuth()
gauth.LoadClientConfigFile(client_secrets_file)
gauth.LocalWebserverAuth()

drive = GoogleDrive(gauth)

# Continue with the rest of your code
folder_name = 'toll_reconciliation_tool'
folder_metadata = {
    'title': folder_name,
    'mimeType': 'application/vnd.google-apps.folder'
}
folder = drive.CreateFile(folder_metadata)
folder.Upload()
folder_id = folder['id']
print(f'Created folder {folder_name} with ID {folder_id}')

# COMMAND ----------

# Criação de subpastas para as camadas bronze, silver e gold
layers = ['bronze', 'silver', 'gold']
layer_ids = {}

for layer in layers:
    layer_metadata = {
        'title': layer,
        'parents': [{'id': folder_id}],
        'mimeType': 'application/vnd.google-apps.folder'
    }
    layer_folder = drive.CreateFile(layer_metadata)
    layer_folder.Upload()
    layer_ids[layer] = layer_folder['id']
    print(f'Created folder {layer} with ID {layer_folder["id"]}')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze
# MAGIC COMMENT 'Database for bronze layer of toll reconciliation tool'
# MAGIC LOCATION 'gdrive:/path/to/toll_reconciliation_tool/bronze/';
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS silver
# MAGIC COMMENT 'Database for silver layer of toll reconciliation tool'
# MAGIC LOCATION 'gdrive:/path/to/toll_reconciliation_tool/silver/';
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS gold
# MAGIC COMMENT 'Database for gold layer of toll reconciliation tool'
# MAGIC LOCATION 'gdrive:/path/to/toll_reconciliation_tool/gold/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze
# MAGIC COMMENT 'Database for bronze layer of toll reconciliation tool'
# MAGIC LOCATION 'dbfs:/Users/renato.maciel.silva@gmail.com/toll-reconciliation-tool/01.bronze/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spark_catalog.silver
# MAGIC LOCATION '/toll-reconciliation-tool/silver/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spark_catalog.gold
# MAGIC LOCATION '/toll-reconciliation-tool/gold/';
