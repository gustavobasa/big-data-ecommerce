#!/usr/bin/env python3
"""
hbase_rest.py - cliente minimalista para a API REST do HBase (só biblioteca padrão).

Por que REST em vez do conector nativo Spark-HBase / TableOutputFormat?
Spark, Flink e HBase aqui rodam em versões diferentes de Hadoop/HBase/Scala,
e casar as versões dos jars de client do HBase com essas imagens é uma fonte
clássica de conflito de dependência (protobuf, guava, shading...). A API REST
do HBase evita isso: qualquer processo que fale HTTP/JSON grava e lê linhas,
então o job Spark (Python) e o job Flink (Java) usam o mesmo endpoint REST
para gravar alertas/insights, sem precisar de client jar nenhum.
"""
import base64
import json
import urllib.request
import urllib.error


def _b64(valor):
    if not isinstance(valor, (bytes, bytearray)):
        valor = str(valor).encode("utf-8")
    return base64.b64encode(valor).decode("ascii")


def criar_tabela(base_url, tabela, familias):
    """Cria a tabela no HBase. Idempotente: se já existir, apenas retorna."""
    familias_xml = "".join(f'<ColumnSchema name="{f}"/>' for f in familias)
    corpo = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<TableSchema name="{tabela}">{familias_xml}</TableSchema>'
    )
    req = urllib.request.Request(
        f"{base_url}/{tabela}/schema",
        data=corpo.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "text/xml", "Accept": "text/xml"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status
    except urllib.error.HTTPError as e:
        if e.code in (403, 405, 409):  # já existe / conflito -> tudo bem
            return e.code
        raise


def put_row(base_url, tabela, row_key, familia, colunas):
    """
    Grava (ou atualiza) uma linha na tabela.
    colunas: dict {nome_da_coluna: valor}
    """
    cells = [
        {"column": _b64(f"{familia}:{nome}"), "$": _b64(valor)}
        for nome, valor in colunas.items()
    ]
    payload = {"Row": [{"key": _b64(row_key), "Cell": cells}]}
    req = urllib.request.Request(
        f"{base_url}/{tabela}/{row_key}",
        data=json.dumps(payload).encode("utf-8"),
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status


def contar_linhas(base_url, tabela):
    """Faz um scan simples e conta as linhas da tabela (usado no verificador)."""
    req = urllib.request.Request(
        f"{base_url}/{tabela}/*",
        headers={"Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            dados = json.loads(r.read().decode("utf-8"))
            return len(dados.get("Row", []))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return 0
        raise