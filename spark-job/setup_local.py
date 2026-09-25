import urllib.request
import json

HBASE_REST_URL = "http://localhost:8083"
TABELAS = ["insights_categoria", "insights_cidade", "flink_alertas"]

def criar_tabela(tabela):
    url = f"{HBASE_REST_URL}/{tabela}/schema"
    schema = {
        "TableSchema": {
            "name": tabela,
            "ColumnSchema": [
                {"name": "cf"}
            ]
        }
    }
    
    req = urllib.request.Request(
        url,
        data=json.dumps(schema).encode('utf-8'),
        headers={
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        },
        method='PUT'
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            print(f"[setup local] Tabela '{tabela}' criada com sucesso! Status: {response.status}")
    except Exception as e:
        print(f"[setup local] Tabela '{tabela}' já existe ou ocorreu um aviso: {e}")

if __name__ == "__main__":
    print("Criando tabelas no HBase...")
    for t in TABELAS:
        criar_tabela(t)
    print("Processo finalizado!")