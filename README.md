# covid19-data-engineering

Um pipeline de ETL para dados de COVID-19, projetado para extrair, transformar e carregar informações em Databricks. Este projeto utiliza Python, arquitetura Medallion e Delta Lake para análises eficientes e insights relevantes sobre a pandemia.

## Índice
1. [Introdução](#introdução)
2. [Objetivos do Projeto](#objetivos-do-projeto)
3. [Estrutura do Pipeline ETL](#estrutura-do-pipeline-etl)
   - [1. Extração dos Dados](#1-extração-dos-dados)
   - [2. Transformação dos Dados](#2-transformação-dos-dados)
   - [3. Carregamento dos Dados](#3-carregamento-dos-dados)
4. [Armazenamento e Formato de Dados](#armazenamento-e-formato-de-dados)
5. [Análise Sumária e Visualizações](#análise-sumária-e-visualizações)
6. [Segurança dos Dados](#segurança-dos-dados)
7. [Monitoramento e Métricas](#monitoramento-e-métricas)
8. [Design System e Documentação](#design-system-e-documentação)
9. [Testes e Ideias Futuras](#testes-e-ideias-futuras)
10. [Limitações e Considerações do Databricks Community](#limitações-e-considerações-do-databricks-community)
11. [Conclusão](#conclusão)

---

## Introdução

Este projeto foi desenvolvido para criar um pipeline de dados COVID-19 que possibilite a análise e extração de insights em uma plataforma escalável e eficiente. Utilizando a arquitetura Medallion e o formato Delta Lake, o pipeline segue as melhores práticas de ETL (Extract, Transform, Load) para preparar os dados para análises descritivas e visuais.

## Objetivos do Projeto

- Extrair dados de uma fonte pública confiável.
- Transformar dados brutos em informações organizadas e preparadas para análise.
- Carregar os dados transformados no Databricks usando o formato Delta Lake.
- Realizar análises sumárias e visualizações para derivar insights relevantes sobre a pandemia de COVID-19.
- Garantir a segurança e a integridade dos dados durante o processo.
- Definir uma estratégia de monitoramento para observabilidade e manutenção do pipeline.

## Estrutura do Pipeline ETL

O pipeline de ETL está dividido em três etapas principais: Extração, Transformação e Carregamento. 

### 1. Extração dos Dados

- **Descrição**: A etapa de extração utiliza a biblioteca `requests` para baixar os dados diretamente de uma URL pública, evitando a necessidade de upload manual.
- **Biblioteca**: `requests`
- **Fonte dos Dados**: [COVID-19 Open Data - Google Cloud](https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv)

### 2. Transformação dos Dados

- **Descrição**: Após a extração, os dados são processados para remover inconsistências e estruturar a informação de forma adequada para a análise. A transformação segue a arquitetura Medallion, dividindo o pipeline em camadas (Bronze, Silver, Gold) para garantir limpeza e qualidade dos dados.
  - **Bronze**: Dados brutos extraídos diretamente da fonte.
  - **Silver**: Dados limpos, com colunas padronizadas e valores ausentes tratados.
  - **Gold**: Dados prontos para análise, contendo agregações e métricas importantes.
- **Tarefas de Transformação**:
  - Limpeza de dados (remoção de duplicatas, tratamento de valores nulos).
  - Estruturação e padronização das colunas.
  - Agregação de métricas e cálculos essenciais para análise.
- **Observação sobre Qualidade dos Dados**: Análise das características dos dados, com tratamento de problemas como valores ausentes ou incorretos, assegurando que os dados transformados estejam prontos para análise.

### 3. Carregamento dos Dados

- **Descrição**: A etapa de carregamento armazena os dados no Databricks usando o formato Delta Lake, que garante controle ACID e permite a escalabilidade e consulta eficiente.
- **Formato de Armazenamento**: Delta Lake
- **Justificativa**: Delta Lake oferece performance e confiabilidade, com suporte para transações ACID e armazenamento eficiente, essencial para uma análise rápida e consistente.

## Armazenamento e Formato de Dados

- **Formato**: Delta Lake
- **Motivo da Escolha**: A escolha do formato Delta Lake permite transações ACID e versionamento, que são úteis para manter a integridade dos dados e facilitar a manutenção e auditoria.

## Análise Sumária e Visualizações

- **Objetivo**: Realizar uma análise descritiva dos dados transformados para identificar padrões e métricas essenciais, como número de casos, mortes e recuperações por região.
- **Ferramentas**: Python, Databricks (para visualização e análise).
- **Estratégia de Análise**: Primeiramente, compreender as características do conjunto de dados, como distribuição de casos por região, e então utilizar essas informações para gerar insights relevantes e visualizações.

## Segurança dos Dados

- **Medidas de Segurança**:
  - Encriptação de dados em trânsito e em repouso.
  - Controle de acesso utilizando permissões na plataforma Databricks.
  - Justificativa: Medidas que asseguram a privacidade e integridade dos dados ao longo do pipeline ETL.

## Monitoramento e Métricas

- **Estratégia de Monitoramento**: O monitoramento garante que o pipeline esteja funcionando corretamente e que os dados carregados estejam consistentes e disponíveis.
  - **Métricas de Monitoramento**:
    - Tempo de execução do pipeline.
    - Taxa de erro e falhas de execução.
    - Consistência e integridade dos dados transformados.
  - **Ferramentas Sugeridas**: Databricks (Jobs Dashboard), Monitoramento de Logs.

## Design System e Documentação

- **Estrutura do Design System**: Utilização de Markdown bem estruturado, com links internos para navegação rápida entre as seções.
- **Visualizações e Diagramas**: Sempre que possível, utilizar fluxogramas e diagramas para ilustrar o fluxo de dados e as transformações.

## Testes e Ideias Futuras

- **Testes Implementados**: Testes unitários para as transformações e validações de dados.
- **Ideias Futuras**: Escalonar o pipeline para um ambiente de produção com monitoração contínua, integração com pipelines CI/CD e melhorias no processamento de dados em tempo real.

## Limitações e Considerações do Databricks Community

- **Limitações**: O Databricks Community tem limitações de performance e recursos, que podem restringir o volume e a frequência de dados processados.
- **Possibilidades no Databricks Pago**: Integração com a AWS para maior escalabilidade, segurança e recursos adicionais de monitoramento.

## Conclusão

Este projeto demonstra um pipeline ETL completo para dados COVID-19 usando Databricks e Python, aplicando boas práticas de mercado como a arquitetura Medallion e armazenamento em Delta Lake. A estrutura flexível e a documentação clara permitem a escalabilidade e adaptação para projetos maiores ou mais complexos.

---
