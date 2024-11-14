# Databricks notebook source
import yaml
from pyspark.sql import SparkSession

# Inicialize a SparkSession (geralmente já está inicializada em notebooks do Databricks)
spark = SparkSession.builder.appName("Verify Silver Columns").getOrCreate()

# Carregar as configurações do arquivo YAML
with open('../config/config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Definindo as variáveis de configuração a partir do config.yaml
silver_path = config['paths']['gold']
 
# Carregar dados da camada Silver e exibir colunas e algumas linhas para verificar o conteúdo
try:
    # Carregar o DataFrame Silver
    silver_df = spark.read.format("delta").load(silver_path)
    
    # Exibir as colunas disponíveis
    print("Colunas disponíveis na camada Silver:", silver_df.columns)
    
    # Mostrar as primeiras 10 linhas para inspecionar o conteúdo
    silver_df.show(100)
    
except Exception as e:
    print(f"Erro ao carregar ou verificar as colunas da camada Silver: {e}")

