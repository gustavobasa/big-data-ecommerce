#!/usr/bin/env python3
"""
setup_hbase_tabelas.py - Cria (uma vez) as tabelas do HBase usadas na Semana 3:
  - insights_categoria / insights_cidade -> gravadas pelo job Spark
  - flink_alertas                        -> gravada pelo job Flink (alertas de trending)

Rodar depois que o container do HBase já estiver de pé:
  docker exec spark-master python3 /jobs/setup_hbase_tabelas.py
"""
import argparse

import hbase_rest

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hbase", default="http://hbase:8080")
    args = ap.parse_args()

    for tabela in ["insights_categoria", "insights_cidade", "flink_alertas"]:
        status = hbase_rest.criar_tabela(args.hbase, tabela, ["cf"])
        print(f"[setup] tabela '{tabela}': status HTTP {status}")