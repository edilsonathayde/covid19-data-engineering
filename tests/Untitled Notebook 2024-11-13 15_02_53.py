# Databricks notebook source
import yaml
import pandas as pd

# Carregar as configurações do arquivo YAML
with open('../config/config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Caminho da camada RAW
raw_path = config['paths']['raw']

# Carregar o arquivo CSV da camada RAW usando Pandas
csv_file_path = f"/dbfs{raw_path}/covid19_aggregated.csv"  # Ajuste o nome do arquivo conforme necessário
raw_df = pd.read_csv(csv_file_path)

# Exibir as primeiras 100 linhas em formato de tabela
raw_df.head(100)
