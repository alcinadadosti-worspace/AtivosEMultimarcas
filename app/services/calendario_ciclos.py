"""
Calendário de ciclos do Grupo Boticário (início, fim e dias úteis).

A tabela abaixo veio da gerência (ago/2026). A coluna de dias úteis JÁ inclui
os +2 dias de extensão que sempre são acrescentados ao fim de cada ciclo.
Para um ano novo, basta acrescentar as linhas em CALENDARIO_CICLOS.

Regra de dia útil usada para saber "em que dia do ciclo estamos":
    segunda a sábado, descontando feriados nacionais fixos + Sexta-feira Santa.
É a regra que fecha com a coluna "Úteis Geral (+2)" da tabela (Carnaval e
Corpus Christi são ponto facultativo e NÃO são descontados).
"""
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

# Quantos dias úteis a mais o ciclo ganha depois da data de fim.
DIAS_EXTENSAO = 2

# (ano, ciclo) -> (inicio, fim, dias_uteis_com_extensao)
CALENDARIO_CICLOS: Dict[Tuple[int, int], Tuple[date, date, int]] = {
    (2026, 1):  (date(2025, 12, 26), date(2026, 1, 18),  21),
    (2026, 2):  (date(2026, 1, 19),  date(2026, 2, 8),   20),
    (2026, 3):  (date(2026, 2, 9),   date(2026, 3, 1),   20),
    (2026, 4):  (date(2026, 3, 2),   date(2026, 3, 22),  20),
    (2026, 5):  (date(2026, 3, 23),  date(2026, 4, 12),  19),
    (2026, 6):  (date(2026, 4, 13),  date(2026, 5, 10),  24),
    (2026, 7):  (date(2026, 5, 11),  date(2026, 5, 24),  14),
    (2026, 8):  (date(2026, 5, 25),  date(2026, 6, 14),  20),
    (2026, 9):  (date(2026, 6, 15),  date(2026, 6, 28),  14),
    (2026, 10): (date(2026, 6, 29),  date(2026, 7, 19),  20),
    (2026, 11): (date(2026, 7, 20),  date(2026, 8, 9),   20),
    (2026, 12): (date(2026, 8, 10),  date(2026, 8, 30),  20),
    (2026, 13): (date(2026, 8, 31),  date(2026, 9, 20),  18),
    (2026, 14): (date(2026, 9, 21),  date(2026, 10, 12), 19),
    (2026, 15): (date(2026, 10, 13), date(2026, 11, 1),  19),
    (2026, 16): (date(2026, 11, 2),  date(2026, 11, 29), 24),
    (2026, 17): (date(2026, 11, 30), date(2026, 12, 25), 23),
}

# Fuso de Brasília (sem horário de verão desde 2019). O servidor do Render
# roda em UTC — sem isso um envio às 22h viraria "amanhã".
_TZ_BRASIL = timezone(timedelta(hours=-3))


# ---------------------------------------------------------------------------
# Datas / dias úteis
# ---------------------------------------------------------------------------

def hoje_brasil() -> date:
    return datetime.now(_TZ_BRASIL).date()


def _pascoa(ano: int) -> date:
    """Domingo de Páscoa (algoritmo de Meeus/Jones/Butcher)."""
    a = ano % 19
    b, c = divmod(ano, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes, dia = divmod(h + l - 7 * m + 114, 31)
    return date(ano, mes, dia + 1)


def feriados(ano: int) -> set:
    """Feriados descontados na contagem de dias úteis (nacionais fixos + Sexta Santa)."""
    fixos = [
        (1, 1),    # Confraternização Universal
        (4, 21),   # Tiradentes
        (5, 1),    # Dia do Trabalho
        (9, 7),    # Independência
        (10, 12),  # N. Sra. Aparecida
        (11, 2),   # Finados
        (11, 15),  # Proclamação da República
        (11, 20),  # Consciência Negra
        (12, 24),  # Véspera de Natal (a tabela da gerência desconta)
        (12, 25),  # Natal
    ]
    dias = {date(ano, m, d) for m, d in fixos}
    dias.add(_pascoa(ano) - timedelta(days=2))  # Sexta-feira Santa
    return dias


def eh_dia_util(d: date) -> bool:
    """Segunda a sábado e não feriado."""
    return d.weekday() != 6 and d not in feriados(d.year)


def dias_uteis_entre(inicio: date, fim: date) -> int:
    """Dias úteis no intervalo fechado [inicio, fim]. 0 se fim < inicio."""
    if fim < inicio:
        return 0
    n = 0
    d = inicio
    while d <= fim:
        if eh_dia_util(d):
            n += 1
        d += timedelta(days=1)
    return n


def _n_esimo_dia_util_apos(d: date, n: int) -> date:
    """n-ésimo dia útil estritamente depois de d."""
    cont = 0
    while cont < n:
        d += timedelta(days=1)
        if eh_dia_util(d):
            cont += 1
    return d


# ---------------------------------------------------------------------------
# Ciclos
# ---------------------------------------------------------------------------

def parse_ciclo(valor: Any, ano_padrao: Optional[int] = None) -> Optional[Tuple[int, int]]:
    """
    '12/2026' -> (2026, 12).  '12' ou 12 -> (ano_padrao ou ano de hoje, 12).
    None se não der para interpretar.
    """
    s = str(valor or "").strip()
    if not s:
        return None
    partes = s.split("/")
    try:
        if len(partes) == 2:
            return int(partes[1]), int(partes[0])
        if len(partes) == 1 and partes[0].isdigit():
            return (ano_padrao or hoje_brasil().year), int(partes[0])
    except ValueError:
        pass
    return None


def formatar_ciclo(ano: int, num: int) -> str:
    return f"{num:02d}/{ano}"


def obter_ciclo(valor: Any) -> Optional[Dict[str, Any]]:
    """Dados de calendário do ciclo ('12/2026'). None se não estiver na tabela."""
    chave = parse_ciclo(valor)
    if not chave or chave not in CALENDARIO_CICLOS:
        return None
    ano, num = chave
    inicio, fim, uteis = CALENDARIO_CICLOS[chave]
    return {
        "ciclo": formatar_ciclo(ano, num),
        "ano": ano,
        "numero": num,
        "inicio": inicio,
        "fim": fim,
        "fim_extensao": _n_esimo_dia_util_apos(fim, DIAS_EXTENSAO),
        "dias_uteis": uteis,
    }


def ciclo_da_data(d: Optional[date] = None) -> Optional[str]:
    """Ciclo cujo período regular [inicio, fim] contém a data ('12/2026')."""
    d = d or hoje_brasil()
    for (ano, num), (inicio, fim, _) in CALENDARIO_CICLOS.items():
        if inicio <= d <= fim:
            return formatar_ciclo(ano, num)
    return None


def posicao_ciclo(valor: Any, data_ref: Optional[date] = None) -> Optional[Dict[str, Any]]:
    """
    Onde estamos dentro do ciclo, na data de referência (padrão: hoje em Brasília).

    Retorna None se o ciclo não estiver no calendário. Caso contrário:
        ciclo, inicio, fim, fim_extensao (ISO), dias_uteis,
        dia_atual        -> dia útil corrente (1..dias_uteis; 0 antes do início)
        dias_concluidos  -> dias úteis já fechados (sem contar hoje)
        dias_restantes   -> dias úteis que faltam, INCLUINDO hoje se for útil
        hoje_util        -> data_ref é dia útil?
        status           -> 'antes' | 'andamento' | 'extensao' | 'encerrado'
        progresso_pct    -> dia_atual / dias_uteis * 100
    """
    info = obter_ciclo(valor)
    if not info:
        return None

    d = data_ref or hoje_brasil()
    total = info["dias_uteis"]
    inicio, fim, fim_ext = info["inicio"], info["fim"], info["fim_extensao"]
    hoje_util = eh_dia_util(d)

    if d < inicio:
        status, dia_atual = "antes", 0
        hoje_util = False
    elif d > fim_ext:
        status, dia_atual = "encerrado", total
        hoje_util = False
    else:
        status = "extensao" if d > fim else "andamento"
        # Segue contando após o fim: os dias de extensão são úteis normais.
        dia_atual = min(total, max(1, dias_uteis_entre(inicio, d)))

    dias_concluidos = max(0, dia_atual - 1) if hoje_util else dia_atual
    if status == "encerrado":
        dias_restantes = 0
    else:
        dias_restantes = max(0, total - dia_atual + (1 if hoje_util else 0))

    return {
        "ciclo": info["ciclo"],
        "inicio": inicio.isoformat(),
        "fim": fim.isoformat(),
        "fim_extensao": fim_ext.isoformat(),
        "data_ref": d.isoformat(),
        "dias_uteis": total,
        "dia_atual": dia_atual,
        "dias_concluidos": dias_concluidos,
        "dias_restantes": dias_restantes,
        "hoje_util": hoje_util,
        "status": status,
        "progresso_pct": round(dia_atual / total * 100, 1) if total else 0.0,
    }
