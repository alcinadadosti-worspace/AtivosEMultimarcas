"""
Envia cards de TESTE da meta diária (e um da meta do ciclo, para comparar)
direto para um Slack ID, ignorando o SLACK_USER_MAP. Usa dados REAIS da
planilha de vendas mais recente da raiz do projeto + metas.xlsx.

Requer SLACK_BOT_TOKEN no ambiente. Sem token, só mostra a prévia (--dry-run).

Uso (na raiz do projeto):
    bash / `!` do Claude:  SLACK_BOT_TOKEN='xoxb-...' python scripts/teste_meta_diaria_slack.py
    PowerShell:            $env:SLACK_BOT_TOKEN='xoxb-...'; python scripts\\teste_meta_diaria_slack.py
Opções:
    --dry-run              só imprime a prévia, não envia nada
    --setor "BRONZE 2"     escolhe o(s) setor(es) (prefixo, pode repetir); padrão: 2 primeiros com meta
    --data-ref 2026-08-25  data considerada como "hoje" (padrão: hoje em Brasília)
    --arquivo caminho.xlsx planilha de vendas (padrão: ConsultaRankingVendas mais recente da raiz)
    --destino U0895CZ8HU7  Slack ID de destino
    --comparar             manda também o card de meta do ciclo do 1º setor (padrão: só meta diária)
"""
import argparse
import os
import sys
from datetime import date
from pathlib import Path

# Terminal do Windows costuma estar em cp1252 e não imprime emoji.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from app.config import SLACK_BOT_TOKEN
from app.services.calendario_ciclos import hoje_brasil, posicao_ciclo
from app.services.slack_service import build_blocks, build_blocks_diario


def _planilha_mais_recente() -> Path:
    cands = sorted(RAIZ.glob("ConsultaRankingVendas*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not cands:
        raise SystemExit("Nenhuma ConsultaRankingVendas*.xlsx na raiz do projeto.")
    return cands[0]


def _carregar_metricas(arquivo: Path, data_ref: date):
    """Sobe a planilha pelo mesmo pipeline da tela (em memória) e devolve
    (ciclo, lista de setores com meta [com recorte 'hoje'], periodo da planilha)."""
    os.environ.setdefault("SLACK_BOT_TOKEN", "")
    from fastapi.testclient import TestClient
    import main

    c = TestClient(main.app)
    with open(arquivo, "rb") as fh:
        r = c.post("/api/upload", files={"file": (arquivo.name, fh,
                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
    if r.status_code != 200:
        raise SystemExit(f"Upload falhou ({r.status_code}): {r.text[:300]}")
    ciclos = c.get("/api/filtros").json()["ciclos"]
    if len(ciclos) != 1:
        raise SystemExit(f"A planilha tem {len(ciclos)} ciclos ({ciclos}); a meta diária precisa de um só.")
    ciclo = ciclos[0]
    ref = data_ref.isoformat()
    dados = c.get("/api/metas/por-setor", params={"ciclos": ciclo, "dia": ref}).json()
    periodo = c.get("/api/metas/periodo-planilha", params={"ciclo": ciclo, "data_ref": ref}).json()
    return ciclo, [m for m in dados if m.get("meta_planilha")], periodo


def _payload(m: dict, periodo: dict) -> dict:
    """Mesmo corpo que a tela monta (montarPayload em meta_setor.html)."""
    mp = m["meta_planilha"]
    return {
        "hoje": m.get("hoje"), "planilha": periodo,
        "receita": m["receita"], "meta_receita": mp["receita"],
        "clientes_ativos": m["clientes_ativos"], "meta_ativo": mp["clientes_ativos"],
        "rpa": m["rpa"], "meta_rpa": mp["rpa"],
        "multimarca": m["percent_multimarcas"], "meta_multimarca": mp["percent_multimarcas"],
        "cabelos": m["percent_cabelos"], "meta_cabelos": mp["percent_cabelos"],
        "make": m["percent_make"], "meta_make": mp["percent_make"],
        "clientes_multimarcas": m["clientes_multimarcas"], "meta_multimarca_qtd": mp["clientes_multimarcas"],
        "clientes_cabelos": m["clientes_cabelos"], "meta_cabelos_qtd": mp["clientes_cabelos"],
        "clientes_make": m["clientes_make"], "meta_make_qtd": mp["clientes_make"],
    }


def _imprimir(blocks: list) -> None:
    for b in blocks:
        if b["type"] == "header":
            print("==", b["text"]["text"])
        elif b["type"] == "section" and "text" in b:
            print(b["text"]["text"])
        elif b["type"] == "section":
            print(" | ".join(f["text"].replace("\n", " ") for f in b["fields"]))
        elif b["type"] == "context":
            print("  ", b["elements"][0]["text"])
    print()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--setor", action="append", default=[])
    ap.add_argument("--data-ref", default=None)
    ap.add_argument("--arquivo", default=None)
    ap.add_argument("--destino", default="U0895CZ8HU7")
    ap.add_argument("--comparar", action="store_true",
                    help="manda também o card de meta do ciclo do 1º setor, para comparar")
    args = ap.parse_args()

    arquivo = Path(args.arquivo) if args.arquivo else _planilha_mais_recente()
    data_ref = date.fromisoformat(args.data_ref) if args.data_ref else hoje_brasil()
    print(f"Planilha: {arquivo.name}")

    ciclo, setores, periodo = _carregar_metricas(arquivo, data_ref)
    pos = posicao_ciclo(ciclo, data_ref)
    if not pos:
        raise SystemExit(f"Ciclo {ciclo} não está no calendário.")
    print(f"Ciclo {ciclo} · ref. {data_ref:%d/%m/%Y} · dia útil {pos['dia_atual']} de {pos['dias_uteis']}"
          f" · faltam {pos['dias_restantes']} · status {pos['status']}")
    print(f"Planilha cobre {periodo.get('data_min')} → {periodo.get('data_max')} ({periodo.get('n_dias')} dias)"
          f" · recorte hoje = {periodo.get('dia_recorte')} · acumulado válido = {periodo.get('acumulado_valido')}")
    if pos["status"] in ("antes", "encerrado"):
        raise SystemExit("Ciclo fora do período — a meta diária não se aplica.")

    if args.setor:
        escolhidos = [m for m in setores
                      if any(m["setor"].upper().startswith(s.upper()) for s in args.setor)]
    else:
        # os 2 setores com mais receita no dia de recorte (para o teste mostrar vendas de hoje)
        escolhidos = sorted(setores, key=lambda m: (m.get("hoje") or {}).get("receita", 0), reverse=True)[:2]
    if not escolhidos:
        raise SystemExit(f"Nenhum setor com meta bate com {args.setor}. Disponíveis: {[m['setor'] for m in setores]}")

    mensagens = []
    for i, m in enumerate(escolhidos):
        dados = _payload(m, periodo)
        mensagens.append((f"[TESTE] Meta Diária — {m['setor']}",
                          build_blocks_diario(m["supervisora"], f"[TESTE] {m['setor']}", dados, pos)))
        # com --comparar, o primeiro setor vai também no formato do ciclo — só quando
        # a planilha é o acumulado do ciclo (com planilha só-do-dia esse card distorce)
        if i == 0 and args.comparar and periodo.get("acumulado_valido", True):
            mensagens.append((f"[TESTE] Meta do Ciclo — {m['setor']}",
                              build_blocks(m["supervisora"], f"[TESTE] {m['setor']}", dados)))

    for texto, blocks in mensagens:
        _imprimir(blocks)

    if args.dry_run:
        print(f"(dry-run) {len(mensagens)} mensagens NÃO enviadas.")
        return 0
    if not SLACK_BOT_TOKEN:
        print("SLACK_BOT_TOKEN não definido — nada foi enviado. Defina o token e rode de novo.")
        return 1

    from slack_sdk import WebClient
    client = WebClient(token=SLACK_BOT_TOKEN)
    try:
        channel_id = client.conversations_open(users=[args.destino])["channel"]["id"]
    except Exception:
        channel_id = args.destino

    for texto, blocks in mensagens:
        try:
            client.chat_postMessage(channel=channel_id, text=texto, blocks=blocks)
            print(f"OK  -> {texto}")
        except Exception as exc:
            print(f"ERRO {texto}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
