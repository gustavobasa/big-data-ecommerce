#!/usr/bin/env python3
"""
Gerador de eventos JSON de e-commerce (cliques, carrinho, compra, entrega).

Cada linha do arquivo de saída é um JSON independente (JSON Lines), ideal
para ser lido linha a linha pelo Flume (taildir/exec).

Características pensadas para as etapas seguintes do pipeline:
  - event_time : instante em que o evento ocorreu (base para watermarks no Flink)
  - late_prob  : fração de eventos emitidos FORA DE ORDEM (event_time atrasado)
  - sessões com carrinho que nunca viram compra -> "carrinho abandonado"
  - produtos com popularidade desigual (Zipf-like) -> "trend topics"
  - pedidos com ciclo de vida de entrega, com atrasos/extravios -> logística
  - duplicatas ocasionais -> permite deduplicação por event_id no Spark

Uso:
  python3 gerador.py --out /var/log/ecommerce/events.log --rate 20
"""
import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

CATALOGO = [
    ("P001", "smartphone", 1899.90), ("P002", "smartphone", 2499.00),
    ("P003", "notebook", 3599.00), ("P004", "notebook", 5299.00),
    ("P005", "fone_ouvido", 199.90), ("P006", "fone_ouvido", 349.90),
    ("P007", "tv", 2799.00), ("P008", "tv", 4199.00),
    ("P009", "eletrodomestico", 899.00), ("P010", "eletrodomestico", 1299.00),
    ("P011", "livro", 49.90), ("P012", "livro", 79.90),
    ("P013", "moda", 129.90), ("P014", "moda", 249.90),
    ("P015", "games", 299.90), ("P016", "games", 449.90),
]
# Popularidade decrescente: poucos produtos concentram a maior parte dos cliques
PESOS = [1.0 / (i + 1) for i in range(len(CATALOGO))]

CIDADES = [
    ("Juazeiro do Norte", "CE"), ("Fortaleza", "CE"), ("Recife", "PE"),
    ("Salvador", "BA"), ("São Paulo", "SP"), ("Rio de Janeiro", "RJ"),
    ("Belo Horizonte", "MG"), ("Brasília", "DF"), ("Porto Alegre", "RS"),
]

# Ciclo de vida da entrega
ETAPAS_ENTREGA = ["PEDIDO_CONFIRMADO", "SEPARADO", "ENVIADO", "EM_TRANSITO", "SAIU_PARA_ENTREGA"]
DESFECHOS = [("ENTREGUE", 0.85), ("ATRASADO", 0.10), ("EXTRAVIADO", 0.05)]

rodando = True


def _parar(*_):
    global rodando
    rodando = False


def agora():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


class Gerador:
    def __init__(self, late_prob, max_late_s):
        self.late_prob = late_prob
        self.max_late_s = max_late_s
        self.sessoes = {}  # session_id -> estado da sessão
        self.pedidos = {}  # order_id -> estado da entrega

    # ---------- montagem do evento ----------
    def _evento(self, tipo, sess, **extra):
        ev_time = agora()
        if random.random() < self.late_prob:  # evento "velho": fora de ordem
            ev_time -= timedelta(seconds=random.uniform(1, self.max_late_s))
        ev = {
            "event_id": str(uuid.uuid4()),
            "event_type": tipo,
            "event_time": iso(ev_time),   # tempo do evento (usado no Flink)
            "ingest_time": iso(agora()),  # tempo de geração
            "user_id": sess["user_id"],
            "session_id": sess["session_id"],
            "city": sess["city"],
            "state": sess["state"],
            "device": sess["device"],
        }
        ev.update(extra)
        return ev

    def _nova_sessao(self):
        cidade, uf = random.choice(CIDADES)
        s = {
            "session_id": "S" + uuid.uuid4().hex[:10],
            "user_id": f"U{random.randint(1, 500):04d}",
            "city": cidade, "state": uf,
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "carrinho": {},  # product_id -> (qtd, preco)
            "cliques_restantes": random.randint(2, 10),
            "vai_comprar": random.random() < 0.30,  # ~70% abandonam
            "fase": "navegando",
        }
        self.sessoes[s["session_id"]] = s
        return s

    # ---------- passos ----------
    def _passo_sessao(self):
        if not self.sessoes or random.random() < 0.15:
            sess = self._nova_sessao()
        else:
            sess = random.choice(list(self.sessoes.values()))

        if sess["fase"] == "navegando":
            if sess["cliques_restantes"] > 0:
                sess["cliques_restantes"] -= 1
                pid, cat, preco = random.choices(CATALOGO, weights=PESOS)[0]
                if random.random() < 0.35:  # 35% dos cliques viram add_to_cart
                    qtd = random.randint(1, 3)
                    sess["carrinho"][pid] = (qtd, preco)
                    return self._evento("add_to_cart", sess, product_id=pid,
                                        category=cat, price=preco, quantity=qtd)
                return self._evento("click", sess, product_id=pid, category=cat, price=preco)
            sess["fase"] = "decidindo"

        if sess["fase"] == "decidindo":
            if sess["carrinho"] and sess["vai_comprar"]:
                sess["fase"] = "comprado"
                return self._checkout(sess)
            # abandona: sessão some sem checkout (carrinho abandonado)
            self.sessoes.pop(sess["session_id"], None)
            return None

        if sess["fase"] == "comprado":
            self.sessoes.pop(sess["session_id"], None)
        return None

    def _checkout(self, sess):
        order_id = "O" + uuid.uuid4().hex[:10]
        itens = [{"product_id": p, "quantity": q, "price": pr}
                 for p, (q, pr) in sess["carrinho"].items()]
        total = round(sum(i["quantity"] * i["price"] for i in itens), 2)
        self.pedidos[order_id] = {"sess": sess, "etapa": 0,
                                  "prox": time.time() + random.uniform(2, 6)}
        return self._evento("purchase", sess, order_id=order_id, items=itens, total=total,
                            payment=random.choice(["pix", "cartao", "boleto"]))

    def _passo_entrega(self):
        agora_s = time.time()
        for oid, p in list(self.pedidos.items()):
            if agora_s < p["prox"]:
                continue
            p["prox"] = agora_s + random.uniform(2, 8)
            if p["etapa"] < len(ETAPAS_ENTREGA):
                status = ETAPAS_ENTREGA[p["etapa"]]
                p["etapa"] += 1
            else:
                r, acc, status = random.random(), 0.0, "ENTREGUE"
                for nome, prob in DESFECHOS:
                    acc += prob
                    if r <= acc:
                        status = nome
                        break
                self.pedidos.pop(oid)
            return self._evento("delivery_status", p["sess"], order_id=oid,
                                delivery_status=status,
                                carrier=random.choice(["Correios", "Jadlog", "Loggi"]))
        return None

    def proximo(self):
        ev = self._passo_entrega() if (self.pedidos and random.random() < 0.25) else None
        return ev or self._passo_sessao()


def main():
    ap = argparse.ArgumentParser(description="Gerador de eventos JSON de e-commerce")
    ap.add_argument("--out", default="events.log", help="arquivo de saída (append)")
    ap.add_argument("--rate", type=float, default=10, help="eventos por segundo")
    ap.add_argument("--max-events", type=int, default=0, help="0 = infinito")
    ap.add_argument("--late-prob", type=float, default=0.10, help="prob. de evento fora de ordem")
    ap.add_argument("--max-late-s", type=float, default=30, help="atraso máx. (s) dos eventos tardios")
    ap.add_argument("--dup-prob", type=float, default=0.01, help="prob. de evento duplicado")
    ap.add_argument("--seed", type=int, default=None, help="semente (reprodutibilidade)")
    ap.add_argument("--stdout", action="store_true", help="também imprime no terminal")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
    signal.signal(signal.SIGINT, _parar)
    signal.signal(signal.SIGTERM, _parar)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    g = Gerador(args.late_prob, args.max_late_s)
    intervalo = 1.0 / args.rate
    total = 0

    # buffering=1 -> line-buffered: cada evento vai ao disco na hora (Flume vê em tempo real)
    with open(args.out, "a", buffering=1, encoding="utf-8") as f:
        while rodando and (args.max_events == 0 or total < args.max_events):
            inicio = time.time()
            ev = g.proximo()
            if ev:
                linha = json.dumps(ev, ensure_ascii=False)
                f.write(linha + "\n")
                total += 1
                if args.stdout:
                    print(linha, flush=True)
                if random.random() < args.dup_prob:  # duplicata (semântica at-least-once)
                    f.write(linha + "\n")
            time.sleep(max(0.0, intervalo - (time.time() - inicio)))
    print(f"[gerador] encerrado. {total} eventos escritos em {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
