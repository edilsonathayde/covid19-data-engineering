# src/etl/transform.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, sum, avg, max, min, stddev, count
from pyspark.sql.types import DateType  


def transform_raw_to_bronze(raw_data_path, bronze_data_path):
    """
    Função para transformar os dados da camada RAW para a camada BRONZE.
    
    :param raw_data_path: Caminho para os dados na camada RAW
    :param bronze_data_path: Caminho onde os dados serão salvos na camada BRONZE
    """
    # Iniciar o SparkSession
    spark = SparkSession.builder.appName("COVID19 Data Transformation").getOrCreate()
    
    # Carregar os dados da camada RAW
    raw_df = spark.read.csv(raw_data_path, header=True, inferSchema=True)
    
    # Salvar o DataFrame na camada BRONZE em formato Delta
    raw_df.write.format("delta").mode("overwrite").save(bronze_data_path)
    
    return f"Dados salvos na camada BRONZE em: {bronze_data_path}"



def transform_bronze_to_silver(bronze_data_path, silver_data_path):
    """
    Transforma os dados da camada BRONZE para a camada SILVER aplicando tratamentos.

    :param bronze_data_path: Caminho para os dados na camada BRONZE
    :param silver_data_path: Caminho onde os dados serão salvos na camada SILVER
    :return: Caminho do arquivo salvo na camada SILVER
    """
    # Inicializar a SparkSession
    spark = SparkSession.builder.appName("COVID19 BRONZE to SILVER Transformation").getOrCreate()
    
    # Carregar dados da camada BRONZE
    bronze_df = spark.read.format("delta").load(bronze_data_path)
    
    # 1. Tratamento de Dados Faltantes
    # Remover colunas com todos os valores nulos
    non_null_columns = [column for column in bronze_df.columns if bronze_df.select(column).dropna().count() > 0]
    cleaned_df = bronze_df.select(*non_null_columns)
    
    # Remover linhas com valores nulos em colunas essenciais ('location_key' e 'date')
    cleaned_df = cleaned_df.dropna(subset=["location_key", "date"])
    
    # 2. Conversão de Tipos
    # Converter `date` para o tipo de data
    transformed_df = cleaned_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    
    # Converter colunas de contagem acumulada e mobilidade para `double` para cálculos futuros
    cumulative_columns = [
        "cumulative_confirmed", "cumulative_deceased", "cumulative_recovered"
    ]
    mobility_columns = [col_name for col_name in cleaned_df.columns if col_name.startswith("mobility_")]
    
    for column in cumulative_columns + mobility_columns:
        if column in cleaned_df.columns:
            transformed_df = transformed_df.withColumn(column, col(column).cast("double"))
    
    # 3. Padronização de Dados Numéricos
    # Normalizar dados de mobilidade para a escala de 0 a 1
    for column in mobility_columns:
        if column in transformed_df.columns:
            transformed_df = transformed_df.withColumn(column, (col(column) + 100) / 200)
    
    # 4. Logs Básicos
    count_before = bronze_df.count()
    count_after = transformed_df.count()
    print(f"Linhas antes da transformação: {count_before}")
    print(f"Linhas após a transformação: {count_after}")
    
    # Salvar na camada SILVER em formato Delta
    transformed_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_data_path)

    print(f"Dados transformados e salvos na camada SILVER em: {silver_data_path}")
    return silver_data_path


def transform_silver_to_gold(silver_data_path, gold_data_path):
    """
    Transforma os dados da camada SILVER para a camada GOLD com foco em métricas chave e indicadores adicionais.

    :param silver_data_path: Caminho para os dados na camada SILVER
    :param gold_data_path: Caminho onde os dados serão salvos na camada GOLD
    """
    # Inicialize a SparkSession
    spark = SparkSession.builder.appName("COVID19 Summary Analysis").getOrCreate()

    # Carregar dados da camada SILVER
    silver_df = spark.read.format("delta").load(silver_data_path)
    
    # Colunas selecionadas para análise
    relevant_columns = [
        "location_key", "date",
        "new_confirmed", "new_deceased", "new_recovered",
        "cumulative_confirmed", "cumulative_deceased", "cumulative_recovered",
        "new_persons_vaccinated", "cumulative_persons_vaccinated",
        "new_hospitalized_patients", "cumulative_hospitalized_patients",
        "mobility_retail_and_recreation"
    ]
    
    # Selecionar colunas relevantes
    selected_df = silver_df.select(*[col for col in relevant_columns if col in silver_df.columns])
    
    # Agregar dados por região (`location_key`) e data (`date`)
    summary_df = selected_df.groupBy("location_key", "date").agg(
        sum("new_confirmed").alias("total_new_confirmed"),
        sum("new_deceased").alias("total_new_deceased"),
        sum("new_recovered").alias("total_new_recovered"),
        max("cumulative_confirmed").alias("total_cumulative_confirmed"),
        max("cumulative_deceased").alias("total_cumulative_deceased"),
        max("cumulative_recovered").alias("total_cumulative_recovered"),
        sum("new_persons_vaccinated").alias("total_new_vaccinated"),
        max("cumulative_persons_vaccinated").alias("total_cumulative_vaccinated"),
        sum("new_hospitalized_patients").alias("total_new_hospitalized"),
        max("cumulative_hospitalized_patients").alias("total_cumulative_hospitalized"),
        avg("mobility_retail_and_recreation").alias("avg_mobility_retail")
    )
    
    # Salvar na camada GOLD em formato Delta
    summary_df.write.format("delta").mode("overwrite").save(gold_data_path)
    print(f"Dados transformados e salvos na camada GOLD em: {gold_data_path}")


