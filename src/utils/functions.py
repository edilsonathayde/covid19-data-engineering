# scr/utils/functions.py

from datetime import datetime

import pyspark
from pyspark.sql.functions import date_format, lit, unix_timestamp
from pyspark.sql.types import IntegerType, FloatType, StringType, DateType, TimestampType, DoubleType

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

import logging

import re

import requests
import os

def convert_table_name(table_name):
    return table_name.replace(".", "_")


def add_metadata(df: DataFrame) -> DataFrame: 
    df_with_metadata = df.withColumn("last_update", F.current_timestamp())
    return df_with_metadata


def add_metadata_df(df: DataFrame) -> DataFrame: 
    df_with_metadata = df.withColumn("last_update", F.current_timestamp())
    return df_with_metadata


def add_month_key(df, date_column_name):
    df_with_month_key = df.withColumn(
        "month_key", date_format(df[date_column_name], "yyyyMM").cast(IntegerType())
    )
    return df_with_month_key


def get_query(table_name, hdfs_source, prefix_layer_name_source, output_tabela_name, tables_queries):
    if table_name in tables_queries:
        return tables_queries[table_name].format(
            hdfs_source=hdfs_source, prefix_layer_name_source=prefix_layer_name_source, output_tabela_name=output_tabela_name 
        )
    else:
        raise ValueError(f"No query found for table: {table_name}")

# Função para tentar inferir o tipo de cada coluna
def infer_and_cast_column(df):
    for column in df.columns:
        # Verifica se a coluna contém valores que são numéricos
        # Se sim, tenta converter para Integer ou Float
        if df.filter(F.col(column).rlike("^[0-9]+$")).count() > 0:  # Verifica se é numérico (inteiro)
            df = df.withColumn(column, F.col(column).cast(IntegerType()))
        elif df.filter(F.col(column).rlike("^[0-9]*\\.?[0-9]+$")).count() > 0:  # Verifica se é numérico (decimal)
            df = df.withColumn(column, F.col(column).cast(FloatType()))
        # Verifica se a coluna tem padrão de data
        elif df.filter(F.col(column).rlike("^(\\d{4})-(\\d{2})-(\\d{2})$")).count() > 0:  # padrão YYYY-MM-DD
            df = df.withColumn(column, F.to_date(column, 'yyyy-MM-dd'))
        elif df.filter(F.col(column).rlike("^(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})$")).count() > 0:  # padrão YYYY-MM-DD HH:MM:SS
            df = df.withColumn(column, F.to_timestamp(column, 'yyyy-MM-dd HH:mm:ss'))
        elif column == 'last_update':
            df = df.withColumn(column, F.to_timestamp(column, 'yyyy-MM-dd HH:mm:ss'))
        # Caso contrário, assume que é String
        else:
            df = df.withColumn(column, F.col(column).cast(StringType()))
    
    return df

# Função para baixar o arquivo do link
def download_file_from_url(url, file_name, dbfs_path):
    """
    Baixa um arquivo CSV de uma URL e o salva no caminho especificado do DBFS.
    Limpa a pasta de destino antes de salvar o novo arquivo.

    Parâmetros:
    - url: URL de onde o arquivo CSV será baixado
    - file_name: Nome final do arquivo para salvar
    - dbfs_path: Caminho do DBFS onde o arquivo será salvo

    Retorna:
    - O caminho completo do arquivo salvo

    Lança:
    - Exception: Se houver erro ao baixar ou salvar o arquivo
    """
    # Realiza o download
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro ao baixar o arquivo: {e}")

    # Caminho completo no DBFS
    full_path = f'/dbfs{dbfs_path}'
    if not os.path.exists(full_path):
        os.makedirs(full_path)

    # Salva o arquivo no DBFS
    file_path = os.path.join(full_path, file_name)
    with open(file_path, 'wb') as file:
        file.write(response.content)

    return file_path


# Função para processar tabela
def process_table(spark, query_input, output_full_name):
    """
    Executa uma consulta SQL para processar uma tabela e salva o resultado em formato Delta.

    Parâmetros:
    - spark: Instância de SparkSession
    - query_input: Consulta SQL a ser executada no Spark
    - output_full_name: Caminho completo onde o resultado será salvo no formato Delta

    Lança:
    - Exception: Em caso de erro durante o processamento ou salvamento

    Passos:
    1. Executa a consulta SQL.
    2. Adiciona a coluna de metadados `last_update` para controle de atualização.
    3. Infere e aplica tipos apropriados nas colunas.
    4. Salva o DataFrame resultante no formato Delta, particionado.
    """
    try:
        # Carregar os dados usando a consulta SQL
        df_input_data = spark.sql(query_input)
        
        ## Adicionar metadados ao DataFrame
        #df_with_update_date = F.add_metadata(df_input_data)

        # Adicionar metadados ao DataFrame
        df_with_update_date = add_metadata_df(df_input_data)

        # Adicionar o tipo de cada coluna
        df_with_infer = infer_and_cast_column(df_with_update_date) 
        
        # Escrever o DataFrame no formato Delta, particionado pela coluna 'aggregation_level'
        df_with_infer.write.format("delta").mode("overwrite").save(output_full_name)
        
        logging.info(f"Query '{query_input}' successfully processed and saved to {output_full_name}")
    
    except Exception as e:
        logging.error(f"Error processing query '{query_input}': {str(e)}")


    