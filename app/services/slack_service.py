"""
Slack integration service.

Sends sector goal results as formatted Block Kit messages via Slack DM.
"""
from datetime import date
from typing import Optional

from app.config import SLACK_BOT_TOKEN, SLACK_USER_MAP, SLACK_DEFAULT_USER_ID
from app.services.calendario_ciclos import hoje_brasil
from app.services.metas import acumulado_valido, calcular_meta_diaria, calcular_meta_do_dia


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
    if pct >= 100:
        full = "🟩"
    elif pct >= 60:
        full = "🟨"
    else:
        full = "🟥"
    return full * filled + "⬜" * (largura - filled)


def _fmt_currency(v) -> str:
    try:
        v = float(v)
        return f"R$ {v:,.0f}".replace(",", ".")
    except Exception:
        return "—"


def _fmt_pct(v) -> str:
    """Formata pontos percentuais (86.7 -> '86.7%').

    Os valores chegam SEMPRE em pontos: os reais vêm de percent_* (já ×100) e
    as metas de metas._parse_pct. Não reescalar valores <= 1 — um setor com
    0,8% de IAF Make sairia no card como "80.0%".
    """
    try:
        return f"{float(v):.1f}%"
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


def _linha_metrica(label: str, real_fmt: str, meta_fmt: str, pct: Optional[float], meta_label: str = "meta") -> str:
    if pct is None:
        return f"•  *{label}:*  {real_fmt}  ·  _sem meta_"
    bar = _barra(pct)
    emoji = "✅" if pct >= 100 else ("⚡" if pct >= 60 else "🔴")
    return f"{emoji}  *{label}:* {real_fmt}  ›  {meta_label} {meta_fmt}  {bar}  *{pct:.0f}%*"


_STATUS_TEXTO = {
    "bateu":    "Parabéns, meta batida! 🎉",
    "quase":    "Falta pouco, você consegue! 💪",
    "longe":    "Bora virar o jogo! 🚀",
    "sem_meta": "Sem meta definida",
}


def _status_geral(dados: dict) -> str:
    """Worst pct across indicators with goal — matches UI's piorPct logic."""
    pares = [
        (dados.get("receita", 0),         dados.get("meta_receita")),
        (dados.get("clientes_ativos", 0), dados.get("meta_ativo")),
        (dados.get("rpa", 0),             dados.get("meta_rpa")),
        (dados.get("multimarca", 0),      dados.get("meta_multimarca")),
        (dados.get("cabelos", 0),         dados.get("meta_cabelos")),
        (dados.get("make", 0),            dados.get("meta_make")),
    ]
    pior = None
    for real, meta in pares:
        pct = _pct_atingimento(real, meta)
        if pct is not None:
            pior = pct if pior is None else min(pior, pct)
    if pior is None:
        return "sem_meta"
    if pior >= 100:
        return "bateu"
    if pior >= 60:
        return "quase"
    return "longe"


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

    # Data de Brasília: o Render roda em UTC e date.today() vira "amanhã" após as 21h.
    hoje = hoje_brasil().strftime("%d/%m/%Y")

    status_geral = _status_geral(dados)
    status_msg   = _STATUS_TEXTO.get(status_geral, "—")

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
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{status_msg}*"},
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


# ---------------------------------------------------------------------------
# Meta diária — MESMO layout do card do ciclo (uma linha por indicador com
# barra e %), em até duas partes: 📍 Hoje e 📈 Acumulado no ciclo.
# ---------------------------------------------------------------------------

_STATUS_DIARIO_TEXTO = {
    "batida":   "Parabéns, meta batida! 🎉",
    "no_ritmo": "No ritmo! Mantém o passo 💪",
    "atrasado": "Abaixo do ritmo — bora virar o jogo! 🚀",
    "sem_meta": "Sem meta definida",
}

_STATUS_DIA_TEXTO = {
    "batida":   "Dia batido! Parabéns 🎉",
    "abaixo":   "Dia abaixo da meta — bora recuperar 🚀",
    "sem_meta": "Sem meta definida",
}


def _fmt_data_br(iso: str) -> str:
    try:
        return date.fromisoformat(iso).strftime("%d/%m")
    except Exception:
        return iso or "—"


def _fmt_por_tipo(v, tipo: str) -> str:
    if tipo == "moeda":
        return _fmt_currency(v)
    try:
        return str(int(round(float(v))))
    except Exception:
        return "—"


def _fmt_dia(v, tipo: str) -> str:
    """Valor por dia: moeda inteira; contagens com 1 casa (ex.: 2,1)."""
    if v is None:
        return "—"
    if tipo == "moeda":
        return _fmt_currency(v)
    return f"{float(v):.1f}".replace(".", ",")


def _detalhe_ritmo(item: dict, dias_restantes: int) -> str:
    """Segunda linha do indicador acumulado: ritmo, quanto falta e quanto precisa por dia."""
    tipo = item["tipo"]
    base = f"base {_fmt_dia(item['meta_dia'], tipo)}/dia"
    if item["status"] == "batida":
        return f"↳ meta batida · {base}"
    ritmo = f"ritmo {item['pct_ritmo']:.0f}%" if item["pct_ritmo"] is not None else "início do ciclo"
    esperado = f"esperado até hoje {_fmt_por_tipo(item['esperado'], tipo)}"
    falta = f"falta {_fmt_por_tipo(item['falta'], tipo)}"
    if item["necessario_dia"] is None:
        return f"↳ {ritmo} ({esperado}) · {falta} · sem dias úteis restantes"
    plural = "dia" if dias_restantes == 1 else "dias"
    return (f"↳ {ritmo} ({esperado}) · {falta} → precisa *{_fmt_dia(item['necessario_dia'], tipo)}/dia* "
            f"nos {dias_restantes} {plural} ({base})")


def build_blocks_diario(supervisora: str, setor: str, dados: dict, posicao: dict) -> list:
    """
    Block Kit da *meta diária* (indicadores de metas.INDICADORES_DIARIOS —
    Receita e Clientes Ativos), no mesmo padrão visual do card do ciclo:

    📍 Hoje — vendas do dia (recorte por DataCaptacao) contra a meta do dia
       (meta do ciclo ÷ dias úteis). Só se `dados["hoje"]` vier preenchido.
    📈 Acumulado no ciclo — realizado vs meta + ritmo/quanto precisa por dia.
       Só se a planilha cobre o ciclo desde o início (`dados["planilha"]`,
       ver metas.acumulado_valido); planilha só do dia mostra apenas o Hoje.
    """
    diario = calcular_meta_diaria(dados, posicao)
    do_dia = calcular_meta_do_dia(dados, posicao)
    planilha = dados.get("planilha") or {}
    tem_acumulado = acumulado_valido(planilha, posicao)

    restantes = int(posicao.get("dias_restantes") or 0)
    total = int(posicao.get("dias_uteis") or 0)
    dia_atual = int(posicao.get("dia_atual") or 0)
    plural = "dia útil" if restantes == 1 else "dias úteis"

    if tem_acumulado:
        status_msg = _STATUS_DIARIO_TEXTO.get(diario["status_geral"], "—")
    elif do_dia["itens"]:
        status_msg = _STATUS_DIA_TEXTO.get(do_dia["status_geral"], "—")
    else:
        status_msg = "Sem vendas do dia nem acumulado na planilha"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"⏱️  Meta Diária — {setor}", "emoji": True},
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Supervisora*\n{supervisora or '—'}"},
                {"type": "mrkdwn", "text": f"*Ciclo {posicao.get('ciclo', '—')}*\nDia útil {dia_atual} de {total} · faltam {restantes} {plural}"},
            ],
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*{status_msg}*"}},
        {"type": "divider"},
    ]

    # ── 📍 Hoje ────────────────────────────────────────────────────────────
    if do_dia["itens"]:
        linhas = [f"*📍 Hoje ({_fmt_data_br(do_dia['data'] or '')})*  _meta do dia = meta do ciclo ÷ {total} dias úteis_"]
        for it in do_dia["itens"]:
            linhas.append(_linha_metrica(
                it["label"], _fmt_por_tipo(it["real_dia"], it["tipo"]), _fmt_dia(it["meta_dia"], it["tipo"]),
                it["pct"], meta_label="meta do dia",
            ))
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(linhas)}})
    elif planilha.get("tem_data"):
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "📍 Sem vendas na planilha para a data de referência."}],
        })

    # ── 📈 Acumulado no ciclo ──────────────────────────────────────────────
    if tem_acumulado:
        if do_dia["itens"]:
            blocks.append({"type": "divider"})
        linhas = [f"*📈 Acumulado no ciclo*  _esperado até hoje = {dia_atual}/{total} da meta_"]
        if diario["itens"]:
            for it in diario["itens"]:
                linhas.append(_linha_metrica(
                    it["label"], _fmt_por_tipo(it["real"], it["tipo"]), _fmt_por_tipo(it["meta"], it["tipo"]), it["pct_meta"],
                ))
                linhas.append(_detalhe_ritmo(it, restantes))
        else:
            linhas.append("_Nenhuma meta de Receita ou Clientes Ativos cadastrada para este setor._")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(linhas)}})
    else:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"ℹ️ A planilha cobre só {_fmt_data_br(planilha.get('data_min') or '')} → "
                    f"{_fmt_data_br(planilha.get('data_max') or '')} — o acumulado do ciclo não está disponível. "
                    "Para ver o ritmo acumulado, suba a planilha do ciclo inteiro."
                ),
            }],
        })

    hoje = hoje_brasil().strftime("%d/%m/%Y")
    ref = _fmt_data_br(posicao.get("data_ref", ""))
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"📅 Gerado em {hoje} · referência {ref} · Multimarks Analytics"}],
    })
    return blocks


def enviar_meta_slack(
    supervisora: str,
    setor: str,
    dados: dict,
    modo: str = "ciclo",
    posicao: Optional[dict] = None,
) -> dict:
    """
    Send a formatted sector goal message to a supervisora via Slack DM.

    Args:
        supervisora: Supervisora name (used to look up Slack ID)
        setor: Sector name
        dados: Metrics dict (see build_blocks for keys)
        modo: "ciclo" (meta geral do ciclo — padrão) ou "diario" (ritmo do dia)
        posicao: obrigatório no modo diário — saída de posicao_ciclo

    Returns:
        {"ok": True} on success or {"ok": False, "error": str} on failure
    """
    try:
        client = _get_client()
        user_id = resolver_slack_id(supervisora)
        if modo == "diario":
            if not posicao:
                return {"ok": False, "error": "posicao do ciclo obrigatória no modo diário"}
            blocks = build_blocks_diario(supervisora, setor, dados, posicao)
            texto = f"Meta Diária — {setor} (ciclo {posicao.get('ciclo', '')})"
        else:
            blocks = build_blocks(supervisora, setor, dados)
            texto = f"Resultado de Metas — {setor}"

        # Try to open DM (requires im:write), fall back to user ID directly
        try:
            dm = client.conversations_open(users=[user_id])
            channel_id = dm["channel"]["id"]
        except Exception:
            channel_id = user_id

        client.chat_postMessage(
            channel=channel_id,
            text=texto,
            blocks=blocks,
        )
        return {"ok": True}

    except Exception as exc:
        return {"ok": False, "error": str(exc)}
