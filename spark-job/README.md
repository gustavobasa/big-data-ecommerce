# Semana 3 - Job Spark (ETL + wide dependencies) + Hive + HBase

## O que foi adicionado

- **HBase** e **Hive** no `docker-compose.yml` (serviços `hbase` e `hive`).
- **`spark-job/etl_insights.py`**: lê os mesmos eventos que o Flume grava em
  `flume-output/` (os mesmos que o Flink já consome na Semana 2), faz
  **dedup**, um **JOIN** e **agregações (GROUP BY)** — as três operações que
  disparam *shuffle* no Spark, ou seja, as **dependências largas** pedidas no
  checkpoint — e calcula 4 insights de negócio:
  1. faturamento por categoria de produto
  2. faturamento por cidade/estado
  3. taxa de abandono de carrinho
  4. distribuição de status de entrega
- Os insights são gravados em **Parquet** (viram tabelas externas no **Hive**)
  e em **HBase** (tabelas `insights_categoria` e `insights_cidade`), via a API
  REST do HBase (`hbase_rest.py`).
- O job Flink da Semana 2 (`EcommerceStreamJob.java`) foi ajustado para também
  gravar um **alerta no HBase** (tabela `flink_alertas`) sempre que uma janela
  tiver `total_eventos >= 5` para um produto — é o "alertas do Flink no
  HBase" do checkpoint.

### Por que REST para o HBase, e não o conector nativo do Spark/Flink?

Spark, Flink e HBase aqui rodam em versões diferentes de Hadoop/Scala, e casar
as versões dos jars de client do HBase com essas imagens é uma fonte clássica
de conflito (protobuf, guava, shading...). Usando a API REST do HBase, tanto o
job Spark (Python) quanto o job Flink (Java, com `java.net.http.HttpClient`,
nativo do Java 17) conversam com o HBase por HTTP/JSON, sem precisar de
nenhum client jar do HBase no classpath.
