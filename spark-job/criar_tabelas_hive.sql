-- criar_tabelas_hive.sql
-- Registra os Parquets gravados pelo job Spark (etl_insights.py) como tabelas
-- externas no Hive. Rodar via beeline depois que o Spark já tiver escrito
-- em ./spark-output (montado no container do Hive como somente leitura em
-- /opt/hive/data/warehouse/spark_output):
--
--   docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 \
--       -f /opt/hive/scripts/criar_tabelas_hive.sql

CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

CREATE EXTERNAL TABLE IF NOT EXISTS faturamento_categoria (
  category            STRING,
  faturamento         DOUBLE,
  unidades_vendidas   BIGINT
)
STORED AS PARQUET
LOCATION '/opt/hive/data/warehouse/spark_output/faturamento_categoria';

CREATE EXTERNAL TABLE IF NOT EXISTS faturamento_cidade (
  city          STRING,
  state         STRING,
  faturamento   DOUBLE,
  pedidos       BIGINT
)
STORED AS PARQUET
LOCATION '/opt/hive/data/warehouse/spark_output/faturamento_cidade';

CREATE EXTERNAL TABLE IF NOT EXISTS abandono_carrinho (
  sessoes_com_carrinho    BIGINT,
  sessoes_que_compraram   BIGINT,
  taxa_abandono_pct       DOUBLE
)
STORED AS PARQUET
LOCATION '/opt/hive/data/warehouse/spark_output/abandono_carrinho';

CREATE EXTERNAL TABLE IF NOT EXISTS status_entrega (
  delivery_status   STRING,
  total             BIGINT
)
STORED AS PARQUET
LOCATION '/opt/hive/data/warehouse/spark_output/status_entrega';

-- consultas de conferência (rode manualmente, uma de cada vez, no beeline)
SELECT * FROM faturamento_categoria ORDER BY faturamento DESC;
SELECT * FROM faturamento_cidade ORDER BY faturamento DESC;
SELECT * FROM abandono_carrinho;
SELECT * FROM status_entrega;