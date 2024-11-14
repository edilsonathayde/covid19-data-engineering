# Databricks notebook source
# MAGIC %md
# MAGIC # Análise dos Dados 
# MAGIC
# MAGIC
# MAGIC ### **Análises Descritivas com PySpark**
# MAGIC
# MAGIC Cálculos para as métricas chave como número total de casos, mortes, e recuperações por região.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

# Inicialize a SparkSession, se ainda não estiver inicializada
spark = SparkSession.builder.appName("COVID19 Analysis").getOrCreate()

# Carregar dados da camada GOLD
gold_path = "dbfs:/mnt/ntconsult/gold"
gold_df = spark.read.format("delta").load(gold_path)

# Calcular métricas descritivas por região
summary_df = gold_df.groupBy("location_key").agg(
    sum("total_new_confirmed").alias("total_cases"),
    sum("total_new_deceased").alias("total_deaths"),
    sum("total_new_recovered").alias("total_recoveries"),
    avg("avg_mobility_retail").alias("avg_mobility_retail")
)

# Exibir os dados sumarizados
summary_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Transferir Dados para Pandas para Visualização**
# MAGIC
# MAGIC Para criar gráficos no Databricks, converta o DataFrame do PySpark para um DataFrame do Pandas. Isso facilita o uso de bibliotecas de visualização como o Matplotlib e o Plotly.

# COMMAND ----------

# Convertendo o DataFrame Spark para Pandas
summary_pd = summary_df.toPandas()


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Criando Visualizações no Notebook do Databricks** 
# MAGIC
# MAGIC _Agora que os dados estão em um DataFrame do Pandas, você pode criar gráficos. Vamos usar o Matplotlib e o Plotly para gerar algumas visualizações._
# MAGIC
# MAGIC
# MAGIC **Exemplo de Visualizações com Matplotlib**
# MAGIC
# MAGIC **a. Gráfico de Barras para Casos, Mortes e Recuperações por Região**

# COMMAND ----------

import pandas as pd

# Agrupar dados por data para análise temporal
time_series_df = gold_df.groupBy("date").agg(
    sum("total_new_confirmed").alias("daily_cases"),
    sum("total_new_deceased").alias("daily_deaths"),
    sum("total_new_recovered").alias("daily_recoveries")
)

# Converter para Pandas para visualização temporal
time_series_pd = time_series_df.toPandas()
time_series_pd['date'] = pd.to_datetime(time_series_pd['date'])

# Plotar a evolução ao longo do tempo
plt.figure(figsize=(12, 6))
plt.plot(time_series_pd["date"], time_series_pd["daily_cases"], color='blue', label='Casos Diários')
plt.plot(time_series_pd["date"], time_series_pd["daily_deaths"], color='red', label='Mortes Diárias')
plt.plot(time_series_pd["date"], time_series_pd["daily_recoveries"], color='green', label='Recuperações Diárias')
plt.xlabel('Data')
plt.ylabel('Número Diário')
plt.title('Evolução Diária de Casos, Mortes e Recuperações')
plt.legend()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC b. Gráfico de Linha para a Mobilidade Média por Região

# COMMAND ----------

# Configuração do gráfico de linha
plt.figure(figsize=(12, 6))
plt.plot(summary_pd["location_key"], summary_pd["avg_mobility_retail"], marker='o', color='purple')
plt.xlabel('Região')
plt.ylabel('Mobilidade Média')
plt.title('Mobilidade Média em Comércio e Lazer por Região')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Exemplo de Visualização com Plotly**
# MAGIC
# MAGIC Plotly permite criar gráficos interativos que são ideais para análises exploratórias.

# COMMAND ----------

import plotly.express as px

# Gráfico de dispersão interativo para casos e mortes
fig = px.scatter(summary_pd, x="total_cases", y="total_deaths", size="total_recoveries",
                 color="location_key", hover_name="location_key",
                 title="Casos e Mortes por Região com Tamanho Proporcional às Recuperações")
fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC **1. Gráfico de Barras Empilhado para Casos, Mortes e Recuperações por Região**
# MAGIC
# MAGIC Esse gráfico empilhado facilita a comparação entre as regiões, mantendo as proporções visíveis.

# COMMAND ----------

import plotly.express as px

# Preparando dados em formato longo para Plotly
summary_long = summary_pd.melt(id_vars="location_key", 
                               value_vars=["total_cases", "total_deaths", "total_recoveries"],
                               var_name="Metric", 
                               value_name="Count")

# Gráfico de barras empilhadas com Plotly
fig = px.bar(summary_long, x="location_key", y="Count", color="Metric",
             title="Casos, Mortes e Recuperações por Região",
             labels={"location_key": "Região", "Count": "Número Total"},
             color_discrete_sequence=["#1f77b4", "#d62728", "#2ca02c"])  # Cores para casos, mortes e recuperações

fig.update_layout(barmode="stack", xaxis_title="Região", yaxis_title="Número Total", 
                  legend_title="Métrica", title_x=0.5)
fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC **2. Gráfico de Linha com Anotações para Evolução Diária**
# MAGIC
# MAGIC Esse gráfico de linha exibe a evolução de casos, mortes e recuperações ao longo do tempo. Adicionar anotações para marcar picos ou eventos importantes pode enriquecer a análise.

# COMMAND ----------

import plotly.graph_objects as go

# Gráfico de linha para evolução diária
fig = go.Figure()

fig.add_trace(go.Scatter(x=time_series_pd["date"], y=time_series_pd["daily_cases"],
                         mode='lines+markers', name="Casos Diários",
                         line=dict(color="#1f77b4", width=2), marker=dict(size=5)))
fig.add_trace(go.Scatter(x=time_series_pd["date"], y=time_series_pd["daily_deaths"],
                         mode='lines+markers', name="Mortes Diárias",
                         line=dict(color="#d62728", width=2), marker=dict(size=5)))
fig.add_trace(go.Scatter(x=time_series_pd["date"], y=time_series_pd["daily_recoveries"],
                         mode='lines+markers', name="Recuperações Diárias",
                         line=dict(color="#2ca02c", width=2), marker=dict(size=5)))

# Configurações adicionais
fig.update_layout(
    title="Evolução Diária de Casos, Mortes e Recuperações",
    xaxis_title="Data",
    yaxis_title="Número Diário",
    legend_title="Métricas",
    title_x=0.5
)

# Check if the filtered DataFrame is not empty before adding annotation
filtered_df = time_series_pd.loc[time_series_pd["date"] == "2022-01-15", "daily_cases"]
if not filtered_df.empty:
    fig.add_annotation(
        x="2022-01-15", y=filtered_df.values[0],
        text="Pico de Casos",
        showarrow=True,
        arrowhead=1
    )
else:
    print("No data available for 2022-01-15 to add annotation.")

fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC **3. Gráfico de Calor (Heatmap) para Casos por Região ao Longo do Tempo**
# MAGIC
# MAGIC O heatmap é ótimo para visualizar intensidades de casos ao longo do tempo em diferentes regiões, revelando padrões sazonais ou de surto.

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# Organizar os dados para heatmap - Pivotando para que as regiões fiquem no eixo y e as datas no eixo x
heatmap_df = gold_df.groupBy("location_key", "date").agg(sum("total_new_confirmed").alias("daily_cases"))
heatmap_pd = heatmap_df.toPandas().pivot("location_key", "date", "daily_cases")

# Configurações do Heatmap com Seaborn
plt.figure(figsize=(14, 8))
sns.heatmap(heatmap_pd, cmap="YlOrRd", cbar_kws={"label": "Casos Diários"})
plt.title("Distribuição de Casos por Região ao Longo do Tempo")
plt.xlabel("Data")
plt.ylabel("Região")
plt.xticks(rotation=45)
plt.show()

