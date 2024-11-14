# Databricks notebook source
# notebook/01_data_extraction

# Importando bibliotecas necessárias
import yaml
from src.utils.file_operations import download_file_from_url

# COMMAND ----------

# Carregar as configurações do arquivo YAML
with open('../config/config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# COMMAND ----------

# Parâmetros de configuração
url = config['data']['url']
file_name = config['data']['file_name'] 
raw_path = config['paths']['raw']  

# COMMAND ----------

# Baixando o arquivo
download_message = download_file_from_url(url, file_name, raw_path)
print(download_message)
