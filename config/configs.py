# config/configs.py

import os
from dotenv import load_dotenv

load_dotenv()

# Carregar variáveis de ambiente
HOST_ADDRESS = os.getenv('HOST_ADDRESS')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

# Caminhos para as camadas de arquitetura Medallion
lake_path = {
    "raw": "/data/raw",
    "bronze": "s3a://bronze/covid19/",
    "silver": "s3a://silver/covid19/",
    "gold": "s3a://gold/covid19/",
}

# Caminhos para as camadas de arquitetura Medallion
prefix_layer_name = {"0": "raw_", "1": "bronze_", "2": "silver_", "3": "gold_"}

# Nome Arquivo
file_name = {"0": "covid19.csv"}

# Nome Nivel Agregação
tabela_name = {"0": "covid19","1": "nive_1","2": "nivel_2","3": "nivel_3"}

# Configs download arquivo
file_data = {
    "url": "https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv",
    "file_name": "covid19.csv",
    "dbfs_path": "/data" 
}


# ************************
# Start Silver Tables
tables_silver = {
    "level_0": f"""SELECT
	location_key, country_name, date,
    population, new_confirmed, new_deceased, new_recovered,
    cumulative_confirmed, cumulative_deceased, cumulative_recovered,
    new_persons_vaccinated, cumulative_persons_vaccinated,
    new_hospitalized_patients, cumulative_hospitalized_patients,
    mobility_retail_and_recreation, aggregation_level
FROM
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}{{output_tabela_name}}` 
WHERE 
    date IS NOT NULL AND 
    location_key IS NOT NULL
    
    """
}


# ************************
# Start Gold Tables
tables_gold = {
    "level_0": f"""SELECT
        country_name, 
        YEAR(date) AS year, 
        MONTH(date) AS month,
        SUM(new_confirmed) AS total_new_confirmed,
        SUM(new_deceased) AS total_new_deceased,
        SUM(new_recovered) AS total_new_recovered,
        SUM(new_persons_vaccinated) AS total_new_vaccinated,
        SUM(new_hospitalized_patients) AS total_new_hospitalized,
        MAX(cumulative_confirmed) AS total_cumulative_confirmed,
        MAX(cumulative_deceased) AS total_cumulative_deceased,
        MAX(cumulative_recovered) AS total_cumulative_recovered,
        MAX(cumulative_persons_vaccinated) AS total_cumulative_vaccinated,
        MAX(cumulative_hospitalized_patients) AS total_cumulative_hospitalized,
        AVG(mobility_retail_and_recreation) AS avg_mobility_retail,
        MAX(population) AS population,
        SUM(new_deceased) / SUM(new_confirmed) AS mortality_rate,  -- Taxa de mortalidade
        SUM(new_recovered) / SUM(new_confirmed) AS recovery_rate  -- Taxa de recuperação
    FROM
        delta.`{{hdfs_source}}{{prefix_layer_name_source}}{{output_tabela_name}}`
    WHERE
        aggregation_level = 0  -- Filtrando pelo nível de agregação (0 neste caso)
        AND date IS NOT NULL
        AND location_key IS NOT NULL
    GROUP BY
        country_name, 
        YEAR(date),
        MONTH(date)
    """
}























    