# Databricks notebook source
# Importando bibliotecas necessárias
import yaml
import os
from pyspark.sql import SparkSession
from src.etl.transform import transform_raw_to_bronze

# COMMAND ----------

# Carregar as configurações do arquivo YAML
with open('../config/config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# COMMAND ----------

# Definindo as variáveis de configuração a partir do config.yaml
raw_path = config['paths']['raw']
bronze_path = config['paths']['bronze']
file_name = config['data']['file_name']

# COMMAND ----------

# Caminho completo do arquivo RAW
raw_data_path = f"{raw_path}/{file_name}"

# COMMAND ----------

# Chama a função de transformação para mover dados de RAW para BRONZE
transform_raw_to_bronze(raw_data_path, bronze_path)

# COMMAND ----------

# Após a transformação, excluir o arquivo da camada RAW
if os.path.exists("/dbfs{raw_data_path}"):
    os.remove(f"/dbfs{raw_data_path}")
    print(f"Arquivo {file_name} removido da camada RAW.")
