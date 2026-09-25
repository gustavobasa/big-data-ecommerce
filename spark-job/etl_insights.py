#!/usr/bin/env python3
"""
etl_insights.py - Job da Semana 3 (ETL Spark + wide dependencies + Hive/HBase).

O que o job faz, na ordem:
  1) lê os eventos JSON que o Flume vai gravando em disco (mesmo diretório
     que o job da Semana 2 já usa no Flink);
  2) remove duplicatas por event_id       -> shuffle = DEPENDÊNCIA LARGA;
  3) faz um JOIN entre os itens comprados e um catálogo produto->categoria
     derivado dos próprios eventos de clique/carrinho -> DEPENDÊNCIA LARGA;
  4) calcula 4 insights de negócio via GROUP BY               -> DEPENDÊNCIA LARGA;
  5) grava os insights em Parquet (para virarem tabelas externas no Hive);
  6) grava um resumo de cada insight como linhas no HBase (via REST).

Uso (dentro do container spark-master, depois do `docker compose up -d`):
  /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      /jobs/etl_insights.py \
      --input /data/flume-output \
      --output /data/spark-output \
      --hbase http://hbase:8080
"""
import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("/jobs")
import hbase_rest  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="/data/flume-output")
    ap.add_argument("--output", default="/data/spark-output")
    ap.add_argument("--hbase", default="http://hbase:8080")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("EcommerceInsightsETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ---------- 1) leitura ----------
    eventos_brutos = spark.read.json(f"{args.input}/*")

    # ---------- 2) dedup (WIDE DEPENDENCY: shuffle por event_id) ----------
    eventos = eventos_brutos.dropDuplicates(["event_id"])
    eventos.cache()

    total_bruto = eventos_brutos.count()
    total_dedup = eventos.count()
    print(f"[ETL] eventos lidos={total_bruto} apos_dedup={total_dedup} "
          f"duplicatas_removidas={total_bruto - total_dedup}")

    # ---------- catálogo produto -> categoria (a partir dos próprios eventos) ----------
    produto_categoria = (
        eventos.where(F.col("category").isNotNull())
        .select("product_id", "category")
        .dropDuplicates(["product_id"])
    )

    compras = eventos.where(F.col("event_type") == "purchase")

    itens = (
        compras
        .select("order_id", "user_id", "city", "state",
                F.explode("items").alias("item"))
        .select(
            "order_id", "user_id", "city", "state",
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.price").alias("price"),
        )
    )

    # ---------- 3) JOIN (WIDE DEPENDENCY) ----------
    itens_categorizados = (
        itens.join(produto_categoria, on="product_id", how="left")
        .withColumn("subtotal", F.col("quantity") * F.col("price"))
    )

    # ---------- 4) insights (GROUP BY = WIDE DEPENDENCY) ----------
    faturamento_categoria = (
        itens_categorizados
        .groupBy(F.coalesce(F.col("category"), F.lit("desconhecida")).alias("category"))
        .agg(F.round(F.sum("subtotal"), 2).alias("faturamento"),
             F.sum("quantity").alias("unidades_vendidas"))
        .orderBy(F.desc("faturamento"))
    )

    faturamento_cidade = (
        compras.groupBy("city", "state")
        .agg(F.round(F.sum("total"), 2).alias("faturamento"),
             F.count("*").alias("pedidos"))
        .orderBy(F.desc("faturamento"))
    )

    add_to_cart = eventos.where(F.col("event_type") == "add_to_cart")
    sessoes_com_carrinho = add_to_cart.select("session_id").distinct()
    sessoes_que_compraram = (
        compras.select("session_id").distinct().withColumn("comprou", F.lit(1))
    )
    abandono_carrinho = (
        sessoes_com_carrinho.join(sessoes_que_compraram, on="session_id", how="left")
        .withColumn("comprou", F.coalesce("comprou", F.lit(0)))
        .agg(
            F.count("*").alias("sessoes_com_carrinho"),
            F.sum("comprou").alias("sessoes_que_compraram"),
        )
        .withColumn(
            "taxa_abandono_pct",
            F.round(100 * (1 - (F.col("sessoes_que_compraram") / F.col("sessoes_com_carrinho"))), 2),
        )
    )

    status_entrega = (
        eventos.where(F.col("event_type") == "delivery_status")
        .groupBy("delivery_status")
        .agg(F.count("*").alias("total"))
        .orderBy(F.desc("total"))
    )

    # ---------- 5) grava em Parquet + CSV (Parquet vira tabela externa no Hive) ----------
    faturamento_categoria.coalesce(1).write.mode("overwrite").parquet(f"{args.output}/faturamento_categoria")
    faturamento_cidade.coalesce(1).write.mode("overwrite").parquet(f"{args.output}/faturamento_cidade")
    abandono_carrinho.coalesce(1).write.mode("overwrite").parquet(f"{args.output}/abandono_carrinho")
    status_entrega.coalesce(1).write.mode("overwrite").parquet(f"{args.output}/status_entrega")

    faturamento_categoria.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{args.output}/csv/faturamento_categoria")
    faturamento_cidade.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{args.output}/csv/faturamento_cidade")

    # ---------- 6) grava resumo no HBase via REST ----------
    hbase_rest.criar_tabela(args.hbase, "insights_categoria", ["cf"])
    hbase_rest.criar_tabela(args.hbase, "insights_cidade", ["cf"])

    for linha in faturamento_categoria.collect():
        hbase_rest.put_row(
            args.hbase, "insights_categoria", linha["category"], "cf",
            {"faturamento": linha["faturamento"], "unidades_vendidas": linha["unidades_vendidas"]},
        )

    for linha in faturamento_cidade.collect():
        chave = f"{linha['state']}#{linha['city']}"
        hbase_rest.put_row(
            args.hbase, "insights_cidade", chave, "cf",
            {"faturamento": linha["faturamento"], "pedidos": linha["pedidos"]},
        )

    print(f"[ETL] insights gravados em {args.output} (Parquet+CSV) e no HBase "
          f"(tabelas insights_categoria / insights_cidade).")
    print("[ETL] Faturamento por categoria:")
    faturamento_categoria.show(truncate=False)
    print("[ETL] Faturamento por cidade:")
    faturamento_cidade.show(truncate=False)
    print("[ETL] Abandono de carrinho:")
    abandono_carrinho.show(truncate=False)
    print("[ETL] Status de entrega:")
    status_entrega.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()