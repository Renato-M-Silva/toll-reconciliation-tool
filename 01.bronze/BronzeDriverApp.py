# Databricks notebook source
# MAGIC %run /toll-reconciliation-tool/00.config/Config

# COMMAND ----------

# Importações
from pyspark.sql.functions import current_date, current_timestamp, expr

# COMMAND ----------

#Nome do database
database = "bronze"
tabela = "tvde_earnings_history"

#path
caminho_arquivo = 'dbfs:/FileStore/toll-reconciliation-tool/uber-2023-dezembro.csv'

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load(caminho_arquivo)

# COMMAND ----------

# incluir colunas de controle
df = df.withColumn("data_carga", current_date())
df = df.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))

# COMMAND ----------

# Grava os dados no formato Delta
df.write \
    .format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{database}.{tabela}')
print("Data loaded successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DETAIL bronze.tvde_earnings_history

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from bronze.tvde_earnings_history
