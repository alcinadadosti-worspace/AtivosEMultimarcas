"""
Slack integration service.

Sends sector goal results as formatted Block Kit messages via Slack DM.
"""
from typing import Optional

from app.config import SLACK_BOT_TOKEN, SLACK_USER_MAP, SLACK_DEFAULT_USER_ID


def _get_client():
    if not SLACK_BOT_TOKEN:
        raise ValueError("SLACK_BOT_TOKEN não configurado")
    from slack_sdk import WebClient
    return WebClient(token=SLACK_BOT_TOKEN)


def resolver_slack_id(supervisora: str) -> str:
    """Return Slack user ID for a supervisora name, falling back to default."""
    if not supervisora:
        return SLACK_DEFAULT_USER_ID
    return SLACK_USER_MAP.get(supervisora.upper().strip(), SLACK_DEFAULT_USER_ID)


def _barra(pct: float, largura: int = 10) -> str:
    filled = round(min(100.0, pct) / 100 * largura)
    return "█" * filled + "░" * (largura - filled)


def _fmt_currency(v) -> str:
    try:
        v = float(v)
        return f"R$ {v:,.0f}".replace(",", ".")
    except Exception:
        return "—"


def _fmt_pct(v) -> str:
    try:
        v = float(v)
        # Values <= 1 are fractions (0.73 = 73%)
        if v <= 1.0:
            v = v * 100
        return f"{v:.1f}%"
    except Exception:
        return "—"


def _pct_atingimento(real, meta) -> Optional[float]:
    try:
        real, meta = float(real), float(meta)
        if meta <= 0:
            return None
        return real / meta * 100
    except Exception:
        return None


def _linha_metrica(label: str, real_fmt: str, meta_fmt: str, pct: Optional[float]) -> str:
    if pct is None:
        return f"• *{label}:* {real_fmt}  _(sem meta)_"
    bar = _barra(pct)
    emoji = "✅" if pct >= 100 else ("⚡" if pct >= 60 else "🔴")
    return f"{emoji} *{label}:* {real_fmt}  ›  meta {meta_fmt}  `{bar}` {pct:.0f}%"


def build_blocks(supervisora: str, setor: str, dados: dict) -> list:
    """
    Build Slack Block Kit blocks for a sector goal card.

    dados keys: receita, meta_receita, clientes_ativos, meta_ativo,
                rpa, meta_rpa, multimarca, meta_multimarca,
                cabelos, meta_cabelos, make, meta_make
    """
    r_rec   = dados.get("receita", 0)
    m_rec   = dados.get("meta_receita")
    r_atv   = dados.get("clientes_ativos", 0)
    m_atv   = dados.get("meta_ativo")
    r_rpa   = dados.get("rpa", 0)
    m_rpa   = dados.get("meta_rpa")
    r_mul   = dados.get("multimarca", 0)
    m_mul   = dados.get("meta_multimarca")
    r_cab   = dados.get("cabelos", 0)
    m_cab   = dados.get("meta_cabelos")
    r_mak   = dados.get("make", 0)
    m_mak   = dados.get("meta_make")

    def pct(r, m):
        return _pct_atingimento(r, m)

    linhas = [
        _linha_metrica("Receita",          _fmt_currency(r_rec), _fmt_currency(m_rec) if m_rec else "—", pct(r_rec, m_rec)),
        _linha_metrica("Clientes Ativos",  str(int(r_atv)),      str(int(m_atv)) if m_atv else "—",      pct(r_atv, m_atv)),
        _linha_metrica("RPA",              _fmt_currency(r_rpa), _fmt_currency(m_rpa) if m_rpa else "—", pct(r_rpa, m_rpa)),
        _linha_metrica("Multimarca %",     _fmt_pct(r_mul),      _fmt_pct(m_mul) if m_mul else "—",      pct(r_mul, m_mul)),
        _linha_metrica("IAF Cabelo %",     _fmt_pct(r_cab),      _fmt_pct(m_cab) if m_cab else "—",      pct(r_cab, m_cab)),
        _linha_metrica("IAF Make %",       _fmt_pct(r_mak),      _fmt_pct(m_mak) if m_mak else "—",      pct(r_mak, m_mak)),
    ]

    from datetime import date
    hoje = date.today().strftime("%d/%m/%Y")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📊  Meta por Setor — {setor}", "emoji": True},
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Supervisora*\n{supervisora or '—'}"},
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(linhas)},
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"📅 Gerado em {hoje} · Multimarks Analytics"}],
        },
    ]
    return blocks


def enviar_meta_slack(
    supervisora: str,
    setor: str,
    dados: dict,
) -> dict:
    """
    Send a formatted sector goal message to a supervisora via Slack DM.

    Args:
        supervisora: Supervisora name (used to look up Slack ID)
        setor: Sector name
        dados: Metrics dict (see build_blocks for keys)

    Returns:
        {"ok": True} on success or {"ok": False, "error": str} on failure
    """
    try:
        client = _get_client()
        user_id = resolver_slack_id(supervisora)
        blocks = build_blocks(supervisora, setor, dados)

        # Try to open DM (requires im:write), fall back to user ID directly
        try:
            dm = client.conversations_open(users=[user_id])
            channel_id = dm["channel"]["id"]
        except Exception:
            channel_id = user_id

        client.chat_postMessage(
            channel=channel_id,
            text=f"Resultado de Metas — {setor}",
            blocks=blocks,
        )
        return {"ok": True}

    except Exception as exc:
        return {"ok": False, "error": str(exc)}
