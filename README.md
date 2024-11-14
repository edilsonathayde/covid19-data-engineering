# COVID-19 Data Engineering Pipeline

Este repositório contém um pipeline de **ETL** (Extração, Transformação e Carregamento) desenvolvido para processar dados da pandemia de **COVID-19**. O pipeline utiliza a arquitetura **Medallion**, dividindo o processo em três camadas: **Bronze**, **Silver** e **Gold**. Os dados são armazenados no formato **Delta Lake** no **Databricks**, garantindo transações **ACID**, alta performance e consultas rápidas para grandes volumes de dados. O repositório está conectado ao **GitHub**, permitindo versionamento e colaboração contínua no desenvolvimento do pipeline.

## Arquitetura de Dados

O pipeline segue a arquitetura **Medallion**, que divide o processo de ETL em três camadas principais, cada uma com um propósito distinto para transformar e organizar os dados de forma eficiente.

### Camada RAW:

A camada **RAW** armazena os dados brutos extraídos diretamente da fonte pública. Os arquivos são baixados e preservados antes de qualquer transformação para garantir a integridade dos dados originais.

### Camada Bronze:

Nessa camada, os dados são processados e convertidos para o formato **Delta Lake**. A transformação inclui a leitura, limpeza e o armazenamento dos dados no formato Delta, que permite consultas rápidas e transações **ACID**.

### Camada Silver:

Após a transformação inicial na camada **Bronze**, os dados são limpos, padronizados e refinados para garantir que estejam prontos para análise. A limpeza envolve a remoção de valores nulos e a conversão de tipos de dados.

### Camada Gold:

Na camada **Gold**, os dados são agregados e preparados para análise avançada. Essa camada contém métricas chave, como o total de casos, mortes e recuperações, prontas para gerar insights e visualizações.

## Gerenciamento de Espaço

Após a transformação dos dados para a camada **Bronze**, o arquivo na camada **RAW** é excluído para liberar espaço de armazenamento, garantindo que o ambiente permaneça eficiente, especialmente ao lidar com grandes volumes de dados.

## Implementação do Pipeline de ETL

O pipeline de ETL é modular, com funções específicas para cada etapa do processo: extração, transformação e carga.

- **Extração**: A função `download_file_from_url()` é responsável por baixar os dados diretamente de uma URL pública para o **Databricks File System (DBFS)**.
- **Transformação**: Funções como `transform_raw_to_bronze()`, `transform_bronze_to_silver()` e `transform_silver_to_gold()` realizam a transformação e limpeza dos dados em cada camada.
- **Carregamento**: Os dados são carregados diretamente no **Delta Lake** utilizando o comando `.write.format("delta").save()`.

## Análise Sumária dos Dados

Após o carregamento na camada **Gold**, realizamos análises descritivas utilizando **PySpark** para agregações e **Pandas** para visualizações. As métricas incluem o número total de casos, mortes e recuperações por região.

## Segurança dos Dados

A segurança dos dados foi uma prioridade durante o desenvolvimento do pipeline. Utilizamos o **AWS Secrets Manager** para o gerenciamento seguro de credenciais sensíveis e implementamos controle de acesso granular no **Databricks**, garantindo que apenas usuários autorizados possam acessar e modificar os dados.

## Monitoramento e Métricas

O monitoramento do pipeline é feito através do **Databricks Jobs Dashboard** e logs, permitindo garantir que o pipeline esteja funcionando corretamente. As métricas monitoradas incluem o tempo de execução, a taxa de erro e a integridade dos dados.

---

## Respostas às Perguntas do Projeto

### 1. Arquitetura de Dados para o Pipeline de ETL

- **a. Explique como você planeja extrair os dados da fonte pública (URL).**
  A extração dos dados é feita diretamente de uma **URL pública** fornecida pelo **COVID-19 Open Data - Google Cloud**. Utilizamos a biblioteca `requests` para baixar o arquivo CSV e armazená-lo no **Databricks File System (DBFS)** para processamento posterior.

- **b. Descreva as etapas de transformação necessárias para preparar os dados para análise.**
  A transformação segue a arquitetura **Medallion** e é dividida em três camadas:
  - **RAW**: Armazenamento dos dados brutos, preservando os dados originais.
  - **Bronze**: A transformação inicial para o formato **Delta Lake**, garantindo dados consistentes e prontos para análise.
  - **Silver**: Limpeza e padronização dos dados, incluindo a remoção de nulos e a conversão de tipos de dados.
  - **Gold**: Agregação e criação de métricas chave, como o total de casos, mortes e recuperações, prontos para geração de insights.

- **c. Explique como você irá carregar os dados transformados na plataforma Databricks.**
  O carregamento é feito diretamente no **Delta Lake**, utilizando o formato **Delta** para garantir transações **ACID**, versionamento e performance otimizada. O comando `.write.format("delta").save()` é utilizado para armazenar os dados na camada adequada.

### 2. Implementação do Pipeline de ETL

- **a. Implemente o processo de ETL utilizando Python/Databricks.**
  O processo de ETL foi implementado de forma modular em **Python/Databricks**, com funções específicas para cada etapa: extração, transformação e carga. Cada notebook no **Databricks** é responsável por uma dessas etapas, tornando o pipeline organizado e fácil de manter.

- **b. Documente o código com comentários claros e concisos.**
  O código está documentado com comentários explicativos para cada função, destacando o que cada uma faz e como elas se encaixam no pipeline geral. A documentação garante que outros desenvolvedores possam entender e modificar o código facilmente.

### 3. Armazenamento na Plataforma Databricks

- **a. Escolha o formato de armazenamento adequado para os dados transformados.**
  O formato escolhido para o armazenamento dos dados transformados é **Delta Lake**. Esse formato oferece as vantagens de transações **ACID** e versionamento, além de garantir alta performance em consultas de grandes volumes de dados.

- **b. Justifique a escolha do formato de armazenamento e descreva como ele suporta consultas e análises eficientes.**
  **Delta Lake** foi escolhido porque oferece suporte a transações **ACID**, garantindo integridade e consistência dos dados. O versionamento dos dados permite auditoria e recuperação de versões anteriores. Além disso, o **Delta Lake** otimiza as consultas, proporcionando alta performance ao lidar com grandes volumes de dados, essencial para o processamento eficiente do pipeline.

### 4. Análise Sumária dos Dados

- **a. Utilize a plataforma Databricks para realizar análises descritivas sobre os dados carregados.**
  Após o carregamento na camada **Gold**, realizamos análises descritivas utilizando **PySpark** para agregações e **Pandas** para visualizações. Calculamos métricas chave, como o número total de casos, mortes e recuperações por região.

- **b. Gere visualizações e métricas chave (ex. número de casos, mortes, recuperações por região).**
  Alguns gráficos foram gerados utilizando **Pandas** e **PySpark** para representar as métricas chave, como número de casos, mortes e recuperações por região. Esses gráficos ajudam a visualizar os padrões e tendências nos dados, facilitando a análise e interpretação. No entanto, acreditamos que o uso de **Tableau** seria uma solução mais robusta e interativa para visualizar esses dados de forma dinâmica, permitindo a criação de dashboards interativos e a exploração dos dados de maneira mais acessível.

  Com mais tempo e recursos, seria possível realizar muitas outras melhorias, como a criação de visualizações mais detalhadas, análises de tendências temporais e comparações entre diferentes regiões ou períodos, utilizando ferramentas como o Tableau para enriquecer ainda mais os insights gerados.

- **c. Documente suas descobertas e insights derivados dos dados.**
  As descobertas incluem insights sobre a evolução da pandemia em diferentes regiões, a mobilidade média por região e a distribuição de casos e mortes ao longo do tempo.

### 5. Implementação de Medidas de Segurança

- **a. Explique como você garante a segurança dos dados durante o processo de ETL.**
  A segurança é garantida pelo uso do **AWS Secrets Manager** para armazenar e gerenciar credenciais sensíveis, além de utilizar criptografia para proteger os dados durante todo o processo de ETL.

- **b. Descreva medidas de segurança implementadas para proteger os dados armazenados na plataforma Databricks.**
  No **Databricks**, implementamos um controle de acesso granular, onde as permissões de leitura e escrita são atribuídas a diferentes usuários com base nas suas necessidades, garantindo que apenas usuários autorizados possam acessar ou modificar dados sensíveis.

### 6. Estratégia de Monitoramento

- **a. Desenhe uma estratégia de monitoramento para o pipeline de ETL.**
  O monitoramento é feito através do **Databricks Jobs Dashboard**, que permite visualizar o status do pipeline e detectar falhas na execução.

- **b. Identifique as métricas chave a serem monitoradas e as ferramentas utilizadas para monitoramento.**
  As métricas chave incluem **tempo de execução**, **taxa de erro** e **integridade dos dados**. O monitoramento é realizado com o **Databricks Jobs Dashboard** e os **logs do Databricks** para identificar falhas e otimizar o processo.

---

Esse código pode ser copiado diretamente para o arquivo `.MD` do seu repositório para fornecer uma documentação clara e detalhada do projeto, respondendo a todas as perguntas de forma organizada e explicativa.
