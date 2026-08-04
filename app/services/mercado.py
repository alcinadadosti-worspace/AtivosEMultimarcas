"""
Mercado (Tabela - Cobertura por cidades) — onde investir para ganhar clientes.

A planilha de cobertura entra UMA vez (persistida) e contribui o que é
estático: população e tier de cada cidade. O numerador da cobertura vem VIVO
da base de revendedores (ConsultaRevendedores) — re-subir a base a cada ciclo
atualiza cobertura, faltantes e farol sem regenerar a planilha de cobertura.

Meta: base >= META_COBERTURA × população ÷ 1000 até o fim do ano (ciclo 17).
O ciclo NÃO é cobrança — é o farol: compara o ritmo necessário no que resta
do ano com o ritmo histórico de cadastro da própria cidade.
"""
import io
import math
import unicodedata
from typing import Any, Dict, List, Optional

import polars as pl

from app.config import (
    MERCADO_COL_CIDADE,
    MERCADO_COL_TIER,
    MERCADO_COL_POP,
    MERCADO_COL_BASE_TOTAL,
    MERCADO_COL_RPA,
    MERCADO_COL_ATIVIDADE,
    META_COBERTURA,
    CICLOS_POR_ANO,
)
from app.config import PED_COL_PESSOA
from app.services.revendedores import _ordem_ciclo, _com_inatividade, ciclos_do_arquivo, _norm_cod


def _chave_cidade(s: str) -> str:
    """Só letras/números, sem acento: casa "OLHO D'AGUA" (base), "OLHO D AGUA"
    (planilha) e "Olho d'Água Grande" (IBGE) na mesma chave."""
    s = unicodedata.normalize("NFD", (s or "").strip().upper())
    return "".join(c for c in s if c.isalnum())


def processar_planilha_mercado(content: bytes, filename: str) -> Dict[str, Any]:
    """Lê a Tabela de Cobertura por cidades e normaliza (1 linha por cidade)."""
    if filename.lower().endswith(".csv"):
        raise ValueError("Use o Excel da Tabela de Cobertura por cidades (.xlsx).")
    try:
        df = pl.read_excel(io.BytesIO(content), infer_schema_length=0)
    except Exception as e:
        raise ValueError(f"Não consegui ler a planilha: {e}")

    # A coluna População pode vir com o acento corrompido dependendo da origem.
    pop_col = next((c for c in df.columns if c.replace("�", "").startswith("Popula")), None)
    if MERCADO_COL_CIDADE not in df.columns or pop_col is None:
        raise ValueError(
            "Esta não parece ser a Tabela de Cobertura por cidades "
            f"(esperava as colunas '{MERCADO_COL_CIDADE}' e '{MERCADO_COL_POP}')."
        )
    for c in (MERCADO_COL_TIER, MERCADO_COL_BASE_TOTAL, MERCADO_COL_RPA, MERCADO_COL_ATIVIDADE):
        if c not in df.columns:
            df = df.with_columns(pl.lit("").alias(c))

    # Números via Float64: célula numérica pode vir serializada como '38053.0',
    # e o cast direto pra Int64 viraria null — cidade sumindo em silêncio.
    def _int(col):
        return pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0).round(0).cast(pl.Int64)

    base = df.with_columns([
        pl.col(MERCADO_COL_CIDADE).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_cidade"),
        pl.col(MERCADO_COL_TIER).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_tier"),
        _int(pop_col).alias("_pop"),
        _int(MERCADO_COL_BASE_TOTAL).alias("_base_ref"),
        pl.col(MERCADO_COL_RPA).cast(pl.Float64, strict=False).fill_null(0.0).alias("_rpa"),
        pl.col(MERCADO_COL_ATIVIDADE).cast(pl.Float64, strict=False).fill_null(0.0).alias("_atividade"),
    ]).select(["_cidade", "_tier", "_pop", "_base_ref", "_rpa", "_atividade"])

    # Fora: linha "Total" (pelo Tier OU pelo nome, caso a coluna Tier falte)
    # e agregados sem população (ex.: OUTRAS CIDADES).
    base = base.filter(
        (pl.col("_cidade") != "")
        & (pl.col("_tier") != "Total")
        & (pl.col("_cidade").str.to_uppercase() != "TOTAL")
        & (pl.col("_pop") > 0)
    )
    if base.is_empty():
        raise ValueError("Nenhuma cidade com população encontrada na planilha.")

    base = base.with_columns(
        pl.col("_cidade").map_elements(_chave_cidade, return_dtype=pl.Utf8).alias("_chave")
    ).unique(subset=["_chave"], keep="first")

    estatisticas = {
        "cidades": base.height,
        "populacao": int(base["_pop"].sum()),
        "arquivo": filename,
    }
    return {"df": base, "estatisticas": estatisticas}


def _num_ciclo(c: str) -> int:
    """'11/2026' -> 11 (0 se formato inesperado)."""
    p = (c or "").split("/")
    return int(p[0]) if len(p) == 2 and p[0].strip().isdigit() else 0


def _ciclo_atual(df_rev: pl.DataFrame, df_ped) -> str:
    """Ciclo mais recente conhecido: do arquivo de pedidos se houver, senão o
    maior CicloPrimeiroPedido da base."""
    if df_ped is not None:
        ciclos = ciclos_do_arquivo(df_ped)
        if ciclos:
            return ciclos[-1]
    vals = [c for c in df_rev["_ciclo_primeiro"].to_list() if c]
    return max(vals, key=_ordem_ciclo) if vals else ""


def _cadastros_por_ciclo(df_rev: pl.DataFrame, n_ciclos: int = 6) -> Dict[str, float]:
    """Ritmo histórico por cidade: cadastros (CicloPrimeiroPedido) nos últimos
    n_ciclos presentes na base, dividido por n_ciclos. Conta só quem sobreviveu
    na base — é o ritmo LÍQUIDO que interessa pra meta."""
    d = df_rev.filter(pl.col("_ciclo_primeiro") != "")
    if d.is_empty():
        return {}
    ciclos = sorted(set(d["_ciclo_primeiro"].to_list()), key=_ordem_ciclo)
    janela = set(ciclos[-n_ciclos:])
    d = d.filter(pl.col("_ciclo_primeiro").is_in(sorted(janela)))
    out: Dict[str, float] = {}
    for r in d.group_by("_cidade").agg(pl.len().alias("n")).iter_rows(named=True):
        out[_chave_cidade(r["_cidade"])] = out.get(_chave_cidade(r["_cidade"]), 0) + r["n"]
    return {k: v / max(1, len(janela)) for k, v in out.items()}


def calcular_mercado(
    df_mercado: pl.DataFrame,
    df_rev: pl.DataFrame,
    df_ped=None,
    meta: float = META_COBERTURA,
    ciclos_ano: int = CICLOS_POR_ANO,
) -> Dict[str, Any]:
    """Cobertura viva por cidade + meta de fim de ano + farol de ritmo."""
    # Base viva por cidade e paradas (>= 3 ciclos sem comprar, mesma conta do alerta).
    com_inat = _com_inatividade(df_rev, df_ped)
    viva: Dict[str, int] = {}
    paradas: Dict[str, int] = {}
    for r in com_inat.select(["_cidade", "_inat"]).iter_rows(named=True):
        k = _chave_cidade(r["_cidade"])
        if not k:
            continue
        viva[k] = viva.get(k, 0) + 1
        if r["_inat"] >= 3:
            paradas[k] = paradas.get(k, 0) + 1

    ritmo_hist = _cadastros_por_ciclo(df_rev)
    ciclo_ref = _ciclo_atual(df_rev, df_ped)
    n_atual = _num_ciclo(ciclo_ref)
    restantes = max(1, ciclos_ano - n_atual)

    cidades: List[Dict[str, Any]] = []
    chaves_mercado = set()
    for r in df_mercado.iter_rows(named=True):
        k = r["_chave"]
        chaves_mercado.add(k)
        base_viva = viva.get(k, 0)
        alvo = math.ceil(meta * r["_pop"] / 1000)
        faltam = max(0, alvo - base_viva)
        cobertura = base_viva * 1000 / r["_pop"]
        ritmo_nec = math.ceil(faltam / restantes) if faltam else 0
        rh = ritmo_hist.get(k, 0.0)
        # Esforço = quantas vezes o próprio ritmo a cidade precisa fazer. É isto
        # que torna a cobrança justa entre cidades de tamanhos diferentes: o
        # gap absoluto favorece cidade grande, o esforço normaliza pela
        # capacidade que a própria cidade demonstra ter.
        esforco = None if (faltam == 0 or rh <= 0) else round(ritmo_nec / rh, 1)
        if faltam == 0:
            farol = "verde"
        elif esforco is None:
            farol = "vermelho"          # não cadastra nada hoje — sem plano novo, não sai
        elif esforco <= 1.0:
            farol = "amarelo"           # o ritmo atual já alcança
        elif esforco <= 2.0:
            farol = "laranja"           # dá com um empurrão
        else:
            farol = "vermelho"          # exige plano diferente
        cidades.append({
            "cidade": r["_cidade"],
            "chave": k,
            "tier": r["_tier"],
            "pop": int(r["_pop"]),
            "base": base_viva,
            "base_ref": int(r["_base_ref"]),
            "cobertura": round(cobertura, 1),
            "alvo": alvo,
            "faltam": faltam,
            "excedente": max(0, base_viva - alvo),
            "ritmo_necessario": ritmo_nec,
            "ritmo_historico": round(rh, 1),
            "esforco": esforco,
            "paradas": paradas.get(k, 0),
            "farol": farol,
            "rpa": round(float(r["_rpa"]), 0),
        })
    # Ordena por dificuldade, não por gap absoluto: senão a cidade pequena que
    # precisa de 5x o próprio ritmo cai pro fim da lista só por ser pequena.
    # (esforço None = não cadastra nada hoje -> o caso mais duro, vai primeiro)
    cidades.sort(key=lambda c: (
        c["faltam"] == 0,
        -(float("inf") if c["esforco"] is None and c["faltam"] else (c["esforco"] or 0)),
        -c["faltam"],
    ))

    fora = sum(v for k, v in viva.items() if k not in chaves_mercado)
    abaixo = [c for c in cidades if c["faltam"] > 0]
    return {
        "meta": meta,
        "ciclo_ref": ciclo_ref,
        "ciclos_restantes": restantes,
        "ciclos_ano": ciclos_ano,
        "resumo": {
            "deficit": sum(c["faltam"] for c in abaixo),
            "ritmo_territorio": sum(c["ritmo_necessario"] for c in abaixo),
            "cidades_abaixo": len(abaixo),
            "cidades_ok": len(cidades) - len(abaixo),
            "fora_do_mapa": fora,
        },
        "cidades": cidades,
        "evolucao": evolucao_base(df_rev),
    }


def _serie_por_ciclo(df_rev: pl.DataFrame, col_norm: str, col_raw: str) -> Optional[Dict[str, int]]:
    """Contagem por ciclo de uma coluna de evento ('11/2026' -> n). Usa a
    normalizada se existir; senão a crua (bases salvas por versões antigas);
    None se a base não tiver a informação."""
    if col_norm in df_rev.columns:
        s = df_rev[col_norm]
    elif col_raw in df_rev.columns:
        s = df_rev[col_raw].cast(pl.Utf8)
    else:
        return None
    out: Dict[str, int] = {}
    for v in s.to_list():
        v = (v or "").strip()
        if v:
            out[v] = out.get(v, 0) + 1
    return out


def evolucao_base(df_rev: pl.DataFrame, n_ciclos: int = 12) -> Dict[str, Any]:
    """A base está crescendo ou caindo? Fluxo por ciclo:
      novas (CicloPrimeiroPedido) + reativadas (CicloReativacao)
      - cessadas (CicloCessamento) = saldo.

    Duas honestidades que a tela precisa carregar:
    - o cadastro guarda só o ÚLTIMO cessamento/reativação de cada cliente, então
      ciclos antigos ficam subcontados (evento sobrescrito por um mais novo);
    - cessamento é administrativo e vem ATRASADO (nasce de inatividade
      acumulada) — os ciclos mais recentes sempre parecem melhores do que são.
    """
    from app.config import REV_COL_CICLO_PRIMEIRO, REV_COL_CICLO_CESSAMENTO, REV_COL_CICLO_REATIVACAO

    novas = _serie_por_ciclo(df_rev, "_ciclo_primeiro", REV_COL_CICLO_PRIMEIRO) or {}
    cessadas = _serie_por_ciclo(df_rev, "_ciclo_cessamento", REV_COL_CICLO_CESSAMENTO)
    reativadas = _serie_por_ciclo(df_rev, "_ciclo_reativacao", REV_COL_CICLO_REATIVACAO)

    todos = set(novas) | set(cessadas or {}) | set(reativadas or {})
    ciclos = sorted(todos, key=_ordem_ciclo)[-n_ciclos:]
    serie = []
    for c in ciclos:
        nv = novas.get(c, 0)
        rv = (reativadas or {}).get(c, 0)
        cs = (cessadas or {}).get(c, 0)
        serie.append({
            "ciclo": c,
            "novas": nv,
            "reativadas": rv,
            "cessadas": cs,
            "saldo": nv + rv - cs,
        })
    ult3 = serie[-3:]
    return {
        "serie": serie,
        "saldo_3ciclos": sum(s["saldo"] for s in ult3),
        "tem_reativacao": reativadas is not None,
        "tem_cessamento": cessadas is not None,
    }


def detalhe_cidade(
    df_mercado: pl.DataFrame,
    df_rev: pl.DataFrame,
    cidade: str,
    df_ped=None,
    meta: float = META_COBERTURA,
) -> Dict[str, Any]:
    """Dentro de uma cidade: revendedores por bairro, quantos estão parados e
    quanto cada bairro comprou (do arquivo de pedidos).

    Sem população por bairro não existe "cobertura do bairro" — o que dá pra
    ler é CONCENTRAÇÃO: onde a base está, onde ela parou, e onde não tem
    ninguém. Bairro com muitos parados = reativar; bairro com base magra num
    município deficitário = recrutar.
    """
    alvo_chave = _chave_cidade(cidade)
    linha = next((r for r in df_mercado.iter_rows(named=True) if r["_chave"] == alvo_chave), None)

    com_inat = _com_inatividade(df_rev, df_ped)
    tem_bairro = "_bairro" in com_inat.columns
    if not tem_bairro:   # base salva antes da coluna de bairro existir
        com_inat = com_inat.with_columns(pl.lit("").alias("_bairro"))

    d = com_inat.filter(
        pl.col("_cidade").map_elements(_chave_cidade, return_dtype=pl.Utf8) == alvo_chave
    )
    if d.is_empty():
        return {"cidade": cidade, "bairros": [], "tem_bairro": tem_bairro, "base": 0}

    # Compras por revendedor no arquivo de pedidos (itens/valor), pra mostrar
    # o peso de cada bairro além da contagem de cabeças.
    compras: Dict[str, Dict[str, float]] = {}
    if df_ped is not None:
        agg = (
            df_ped.with_columns(_norm_cod(PED_COL_PESSOA).alias("_cod"))
            .filter(pl.col("_cod") != "")
            .group_by("_cod")
            .agg([pl.col("_itens").sum().alias("itens"), pl.col("_valor").sum().alias("valor")])
        )
        compras = {r["_cod"]: {"itens": r["itens"], "valor": r["valor"]} for r in agg.iter_rows(named=True)}

    # Agrupa pela mesma chave da cidade (sem acento/pontuação): o cadastro tem
    # o mesmo bairro escrito de várias formas ("ALDEIA KARIRI XOCO" e "ALDEIA
    # KARIRI-XOCÓ"), e sem juntar isso um bairro grande vira vários minúsculos
    # — que a coluna Ação leria como "base magra, recrutar".
    bairros: Dict[str, Dict[str, Any]] = {}
    for r in d.select(["_cod", "_bairro", "_inat"]).iter_rows(named=True):
        nome = (r["_bairro"] or "").strip() or "Não informado"
        chave = _chave_cidade(nome) or "NAOINFORMADO"
        b = bairros.setdefault(chave, {"_grafias": {}, "revendedores": 0, "parados": 0, "itens": 0, "valor": 0.0})
        b["_grafias"][nome] = b["_grafias"].get(nome, 0) + 1
        b["revendedores"] += 1
        if r["_inat"] >= 3:
            b["parados"] += 1
        c = compras.get(r["_cod"])
        if c:
            b["itens"] += int(c["itens"] or 0)
            b["valor"] += float(c["valor"] or 0.0)

    total = d.height
    out = []
    for b in bairros.values():
        grafias = b.pop("_grafias")
        b["bairro"] = max(grafias, key=grafias.get)   # a grafia mais usada representa o grupo
        b["variantes"] = len(grafias)
        b["ativos"] = b["revendedores"] - b["parados"]
        b["share"] = round(b["revendedores"] / total * 100, 1) if total else 0.0
        b["valor"] = round(b["valor"], 2)
        out.append(b)
    out.sort(key=lambda b: -b["revendedores"])

    faltam = 0
    if linha is not None:
        faltam = max(0, math.ceil(meta * linha["_pop"] / 1000) - total)
    return {
        "cidade": linha["_cidade"] if linha is not None else cidade,
        "base": total,
        "faltam": faltam,
        "bairros": out,
        "tem_bairro": tem_bairro,
    }
