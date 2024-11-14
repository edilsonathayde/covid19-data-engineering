# Databricks notebook source
import yaml
import os
from src.etl.transform import transform_bronze_to_silver

# COMMAND ----------

# Carregar as configurações do arquivo YAML
with open('../config/config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# COMMAND ----------

# Definindo as variáveis de configuração a partir do config.yaml
bronze_path = config['paths']['bronze']
silver_path = config['paths']['silver']

# COMMAND ----------

silver_path_full = f"dbfs:{config['paths']['silver']}"  # Completa o caminho para DBFS

# Limpar o diretório da camada Silver no DBFS
dbutils.fs.rm(silver_path_full, recurse=True)
print(f"Todos os dados na camada Silver foram removidos de: {silver_path_full}")

# COMMAND ----------

# Executar a transformação da camada BRONZE para SILVER
transform_bronze_to_silver(bronze_path, silver_path)
