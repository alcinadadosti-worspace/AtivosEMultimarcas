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
from app.services.revendedores import _ordem_ciclo, _com_inatividade, ciclos_do_arquivo


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

    base = df.with_columns([
        pl.col(MERCADO_COL_CIDADE).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_cidade"),
        pl.col(MERCADO_COL_TIER).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_tier"),
        pl.col(pop_col).cast(pl.Int64, strict=False).fill_null(0).alias("_pop"),
        pl.col(MERCADO_COL_BASE_TOTAL).cast(pl.Int64, strict=False).fill_null(0).alias("_base_ref"),
        pl.col(MERCADO_COL_RPA).cast(pl.Float64, strict=False).fill_null(0.0).alias("_rpa"),
        pl.col(MERCADO_COL_ATIVIDADE).cast(pl.Float64, strict=False).fill_null(0.0).alias("_atividade"),
    ]).select(["_cidade", "_tier", "_pop", "_base_ref", "_rpa", "_atividade"])

    # Fora: linha "Total", agregados sem população (ex.: OUTRAS CIDADES).
    base = base.filter(
        (pl.col("_cidade") != "") & (pl.col("_tier") != "Total") & (pl.col("_pop") > 0)
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
        if faltam == 0:
            farol = "verde"
        elif ritmo_nec <= rh:
            farol = "amarelo"       # abaixo da meta, mas o ritmo atual alcança
        else:
            farol = "vermelho"      # precisa acelerar além do histórico
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
            "paradas": paradas.get(k, 0),
            "farol": farol,
            "rpa": round(float(r["_rpa"]), 0),
        })
    cidades.sort(key=lambda c: (-c["faltam"], -c["pop"]))

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
    }
