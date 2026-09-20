#!/usr/bin/env python3
"""
verificar.py - Confere se o Flume entregou no sink de teste os eventos
gerados pelo gerador.py (Semana 1).

Uso (na pasta do projeto, com o gerador e o Flume rodando):
  python verificar.py                    -> confere uma vez
  python verificar.py --minutos 5        -> confere de 15 em 15 s durante 5 min
"""
import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

try:
    sys.stdout.reconfigure(errors="replace")  # evita erro de acento no console Windows
except Exception:
    pass


def linhas(caminho):
    """Lê as linhas não vazias de um arquivo (ignora \\r e espaços)."""
    with open(caminho, "r", encoding="utf-8", errors="replace") as f:
        return [l.strip() for l in f if l.strip()]


def contar(caminho):
    if not Path(caminho).exists():
        return 0
    return len(linhas(caminho))


def arquivos_saida(pasta):
    p = Path(pasta)
    return sorted(x for x in p.iterdir() if x.is_file()) if p.exists() else []


def contar_saida(pasta):
    return sum(contar(a) for a in arquivos_saida(pasta))


def analise_completa(origem, saida):
    src = linhas(origem) if Path(origem).exists() else []
    src_ids = Counter()
    for l in src:
        try:
            src_ids[json.loads(l)["event_id"]] += 1
        except Exception:
            pass

    sink_ids, tipos = Counter(), Counter()
    invalidas = incompletas = total_sink = 0
    arqs = arquivos_saida(saida)
    for a in arqs:
        ls = linhas(a)
        for i, l in enumerate(ls):
            total_sink += 1
            try:
                ev = json.loads(l)
                sink_ids[ev["event_id"]] += 1
                tipos[ev["event_type"]] += 1
            except Exception:
                if i == len(ls) - 1:
                    incompletas += 1   # última linha pode estar sendo escrita agora
                else:
                    invalidas += 1

    fora_da_origem = sum(1 for k in sink_ids if k not in src_ids)
    faltando = sum(max(0, src_ids[k] - sink_ids.get(k, 0)) for k in src_ids)

    print("=" * 60)
    print(f"Linhas no arquivo de origem (data/events.log): {len(src)}")
    print(f"Arquivos no sink (flume-output/)             : {len(arqs)}")
    print(f"Linhas no sink                               : {total_sink}")
    print("-" * 60)
    print("Eventos no sink por tipo:")
    for t, n in tipos.most_common():
        print(f"   {t:<18} {n}")
    print("-" * 60)
    print(f"Linhas com JSON inválido no sink   : {invalidas}")
    print(f"Última linha incompleta (normal)   : {incompletas}")
    print(f"Eventos no sink que NÃO existem na origem: {fora_da_origem}")
    print(f"Eventos da origem ainda não entregues    : {faltando} (pequeno = normal)")
    print("=" * 60)

    ok = total_sink > 0 and invalidas == 0 and fora_da_origem == 0
    print("[OK] Sink recebendo eventos JSON válidos vindos do gerador." if ok
          else "[FALHA] Veja os números acima (sink vazio, JSON inválido ou eventos estranhos).")
    return ok


def monitorar(origem, saida, minutos, intervalo):
    fim = time.time() + minutos * 60
    anterior = None
    cresceu_sempre = True
    print(f"Monitorando por {minutos} min (amostra a cada {intervalo}s). Ctrl+C interrompe.\n")
    print(f"{'hora':<10}{'origem':>10}{'sink':>10}{'diferença':>12}   situação")
    while time.time() < fim:
        o, s = contar(origem), contar_saida(saida)
        sit = "primeira amostra"
        if anterior is not None:
            if s > anterior:
                sit = "sink crescendo (OK)"
            else:
                sit = "SINK PARADO (!)"
                cresceu_sempre = False
        print(f"{time.strftime('%H:%M:%S'):<10}{o:>10}{s:>10}{o - s:>12}   {sit}")
        anterior = s
        time.sleep(intervalo)
    print()
    print("[OK] O sink cresceu em todas as amostras." if cresceu_sempre
          else "[FALHA] Em alguma amostra o sink parou de crescer.")
    return cresceu_sempre


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--origem", default="data/events.log")
    ap.add_argument("--saida", default="flume-output")
    ap.add_argument("--minutos", type=float, default=0, help="0 = confere uma vez")
    ap.add_argument("--intervalo", type=float, default=15)
    a = ap.parse_args()

    if a.minutos > 0:
        ok = monitorar(a.origem, a.saida, a.minutos, a.intervalo)
        print()
    else:
        ok = True
    ok = analise_completa(a.origem, a.saida) and ok
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
