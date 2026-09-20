#!/usr/bin/env python3
"""
status_ambiente.py - Confere se os serviços do ambiente da Semana 1 estão no ar:
Flume (container), HDFS (NameNode + DataNode), Flink (JobManager + TaskManager)
e Spark (Master + Worker).

Uso (na pasta do projeto, depois de 'docker compose up -d'):
  python status_ambiente.py
"""
import argparse
import json
import subprocess
import sys
import urllib.request

try:
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass


def http_json(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def checar_flume():
    r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", "flume-semana1"],
                       capture_output=True, text=True, timeout=20)
    if r.stdout.strip() == "true":
        return True, "container flume-semana1 em execução"
    return False, "container flume-semana1 NÃO está rodando"


def checar_hdfs(base):
    d = http_json(f"{base}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")
    vivos = d["beans"][0]["NumLiveDataNodes"]
    return vivos >= 1, f"NameNode respondendo, DataNodes ativos: {vivos} (esperado: 1)"


def checar_flink(base):
    d = http_json(f"{base}/overview")
    tms, slots = d["taskmanagers"], d["slots-total"]
    return tms >= 1, f"JobManager respondendo, TaskManagers: {tms}, slots: {slots} (esperado: 1 e 2)"


def checar_spark(base):
    d = http_json(f"{base}/json/")
    vivos = d.get("aliveworkers", 0)
    return d.get("status") == "ALIVE" and vivos >= 1, \
        f"Master {d.get('status')}, Workers ativos: {vivos} (esperado: ALIVE e 1)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hdfs", default="http://localhost:9870")
    ap.add_argument("--flink", default="http://localhost:8081")
    ap.add_argument("--spark", default="http://localhost:8080")
    a = ap.parse_args()

    testes = [
        ("Flume", checar_flume),
        ("HDFS ", lambda: checar_hdfs(a.hdfs)),
        ("Flink", lambda: checar_flink(a.flink)),
        ("Spark", lambda: checar_spark(a.spark)),
    ]
    tudo_ok = True
    print("=" * 64)
    for nome, fn in testes:
        try:
            ok, msg = fn()
        except Exception as e:
            ok, msg = False, f"sem resposta ({type(e).__name__}) - ainda iniciando ou fora do ar?"
        tudo_ok &= ok
        print(f"[{'OK' if ok else 'FALHA'}] {nome}: {msg}")
    print("=" * 64)
    print("Ambiente da Semana 1 no ar." if tudo_ok
          else "Algo não está no ar. Espere 1 minuto e rode de novo; se persistir, veja 'docker compose ps'.")
    sys.exit(0 if tudo_ok else 1)


if __name__ == "__main__":
    main()
