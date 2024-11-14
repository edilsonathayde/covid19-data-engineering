# Databricks notebook source
import yaml
import os
from src.etl.transform import transform_silver_to_gold

# COMMAND ----------

# Carregar as configurações do arquivo YAML
with open('../config/config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# COMMAND ----------

# Definindo as variáveis de configuração a partir do config.yaml

silver_path = config['paths']['silver']
gold_path = config['paths']['gold']

# COMMAND ----------

# Executar a transformação da camada BRONZE para SILVER
transform_silver_to_gold(silver_path, gold_path)
