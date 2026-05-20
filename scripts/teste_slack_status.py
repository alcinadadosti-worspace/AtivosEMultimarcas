"""
Envia 3 cards de teste (bateu / quase / longe) direto para um Slack ID,
ignorando o SLACK_USER_MAP. Requer SLACK_BOT_TOKEN no ambiente.

Uso (PowerShell, na raiz do projeto):
    python scripts\teste_slack_status.py
"""
import sys
from pathlib import Path

# Permite rodar de qualquer cwd
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import SLACK_BOT_TOKEN
from app.services.slack_service import build_blocks

DESTINO = "U0895CZ8HU7"

CENARIOS = [
    (
        "TESTE - BATEU",
        {
            "receita": 120000, "meta_receita": 100000,
            "clientes_ativos": 220, "meta_ativo": 200,
            "rpa": 545, "meta_rpa": 500,
            "multimarca": 0.85, "meta_multimarca": 0.80,
            "cabelos": 0.55, "meta_cabelos": 0.50,
            "make": 0.62, "meta_make": 0.55,
        },
    ),
    (
        "TESTE - QUASE",
        {
            "receita": 78000, "meta_receita": 100000,
            "clientes_ativos": 160, "meta_ativo": 200,
            "rpa": 380, "meta_rpa": 500,
            "multimarca": 0.65, "meta_multimarca": 0.80,
            "cabelos": 0.38, "meta_cabelos": 0.50,
            "make": 0.42, "meta_make": 0.55,
        },
    ),
    (
        "TESTE - LONGE",
        {
            "receita": 35000, "meta_receita": 100000,
            "clientes_ativos": 90, "meta_ativo": 200,
            "rpa": 180, "meta_rpa": 500,
            "multimarca": 0.30, "meta_multimarca": 0.80,
            "cabelos": 0.15, "meta_cabelos": 0.50,
            "make": 0.18, "meta_make": 0.55,
        },
    ),
]


def main() -> int:
    if not SLACK_BOT_TOKEN:
        print("ERRO: SLACK_BOT_TOKEN não está definido no ambiente.")
        return 1

    from slack_sdk import WebClient
    client = WebClient(token=SLACK_BOT_TOKEN)

    # Abre DM com o destino (mesmo padrão do slack_service)
    try:
        dm = client.conversations_open(users=[DESTINO])
        channel_id = dm["channel"]["id"]
    except Exception:
        channel_id = DESTINO

    for setor, dados in CENARIOS:
        blocks = build_blocks(supervisora="Teste Visual", setor=setor, dados=dados)
        try:
            client.chat_postMessage(
                channel=channel_id,
                text=f"[TESTE] {setor}",
                blocks=blocks,
            )
            print(f"OK  -> {setor}")
        except Exception as exc:
            print(f"ERRO {setor}: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
