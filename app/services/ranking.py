"""
Reseller ranking service.

Calculates rankings and comparisons for resellers.
"""
from typing import Any, Dict, List, Optional

import polars as pl

from app.config import (
    VENDAS_COL_CICLO,
    VENDAS_COL_SETOR,
    VENDAS_COL_CODIGO_REVENDEDORA,
    VENDAS_COL_NOME_REVENDEDORA,
    VENDAS_COL_QTD_ITENS,
    VENDAS_COL_VALOR,
    VENDAS_COL_GERENCIA,
)


def calcular_ranking_revendedoras(
    df_vendas: pl.DataFrame,
    limite: int = 20,
) -> List[Dict[str, Any]]:
    """
    Calculate top resellers by total value.

    Args:
        df_vendas: Sales DataFrame
        limite: Maximum number of resellers to return

    Returns:
        List of reseller rankings
    """
    # Check if gerencia column exists
    has_gerencia = VENDAS_COL_GERENCIA in df_vendas.columns

    # Group by reseller
    agg_cols = [
        pl.col(VENDAS_COL_NOME_REVENDEDORA).first().alias("nome"),
        pl.col(VENDAS_COL_SETOR).first().alias("setor"),
        pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum().alias("total_itens"),
        pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum().alias("total_valor"),
        pl.col("Marca_BD").n_unique().alias("qtde_marcas"),
        pl.col("Marca_BD").unique().alias("marcas_lista"),
        pl.col(VENDAS_COL_CICLO).n_unique().alias("ciclos_ativos"),
    ]

    if has_gerencia:
        agg_cols.append(pl.col(VENDAS_COL_GERENCIA).first().alias("gerencia"))

    df_ranking = df_vendas.group_by(VENDAS_COL_CODIGO_REVENDEDORA).agg(agg_cols)

    # Sort by value and get top
    df_ranking = df_ranking.sort("total_valor", descending=True).head(limite)

    # Convert to list of dicts
    resultado = []
    for i, row in enumerate(df_ranking.iter_rows(named=True)):
        marcas_list = row["marcas_lista"]
        marcas_str = ", ".join(sorted([m for m in marcas_list if m and m != "DESCONHECIDA"]))

        item = {
            "posicao": i + 1,
            "codigo": row[VENDAS_COL_CODIGO_REVENDEDORA],
            "nome": row["nome"],
            "setor": row["setor"],
            "total_itens": int(row["total_itens"] or 0),
            "total_valor": float(row["total_valor"] or 0),
            "qtde_marcas": row["qtde_marcas"],
            "marcas": marcas_str,
            "ciclos_ativos": row["ciclos_ativos"],
            "is_multimarcas": row["qtde_marcas"] >= 2,
        }

        if has_gerencia:
            item["gerencia"] = row.get("gerencia", "")

        resultado.append(item)

    return resultado


def calcular_evolucao_revendedora(
    df_vendas: pl.DataFrame,
    codigo_revendedora: str,
) -> List[Dict[str, Any]]:
    """
    Calculate a reseller's evolution over cycles.

    Args:
        df_vendas: Sales DataFrame
        codigo_revendedora: Reseller code

    Returns:
        List of cycle-by-cycle metrics
    """
    df_rev = df_vendas.filter(
        pl.col(VENDAS_COL_CODIGO_REVENDEDORA).cast(pl.Utf8) == str(codigo_revendedora)
    )

    if df_rev.is_empty():
        return []

    # Group by cycle
    df_ciclo = df_rev.group_by(VENDAS_COL_CICLO).agg([
        pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum().alias("total_itens"),
        pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum().alias("total_valor"),
        pl.col("Marca_BD").n_unique().alias("qtde_marcas"),
    ]).sort(VENDAS_COL_CICLO)

    resultado = []
    valor_anterior = None

    for row in df_ciclo.iter_rows(named=True):
        valor_atual = float(row["total_valor"] or 0)

        # Calculate variation
        variacao = None
        if valor_anterior is not None and valor_anterior > 0:
            variacao = ((valor_atual - valor_anterior) / valor_anterior) * 100

        resultado.append({
            "ciclo": row[VENDAS_COL_CICLO],
            "total_itens": int(row["total_itens"] or 0),
            "total_valor": valor_atual,
            "qtde_marcas": row["qtde_marcas"],
            "variacao_percentual": round(variacao, 2) if variacao is not None else None,
        })

        valor_anterior = valor_atual

    return resultado


def calcular_comparativo_ciclos(
    df_clientes: pl.DataFrame,
    df_vendas: pl.DataFrame,
    ciclos: List[str],
) -> Dict[str, Any]:
    """
    Calculate comparison metrics between selected cycles.

    Args:
        df_clientes: Clients DataFrame
        df_vendas: Sales DataFrame
        ciclos: List of cycles to compare

    Returns:
        Comparison metrics
    """
    if not ciclos or len(ciclos) == 0:
        return {"ciclos": [], "metricas": []}

    # Sort cycles
    ciclos_ordenados = sorted(ciclos)

    metricas_por_ciclo = []

    for ciclo in ciclos_ordenados:
        df_cli_ciclo = df_clientes.filter(pl.col(VENDAS_COL_CICLO) == ciclo)
        df_ven_ciclo = df_vendas.filter(pl.col(VENDAS_COL_CICLO) == ciclo)

        total_clientes = df_cli_ciclo.select(pl.col("ClienteID").n_unique()).item() or 0
        total_multimarcas = df_cli_ciclo.filter(pl.col("IsMultimarcas")).select(pl.col("ClienteID").n_unique()).item() or 0
        total_itens = df_ven_ciclo.select(pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum()).item() or 0
        total_valor = df_ven_ciclo.select(pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum()).item() or 0

        percent_multi = (total_multimarcas / total_clientes * 100) if total_clientes > 0 else 0

        metricas_por_ciclo.append({
            "ciclo": ciclo,
            "clientes_ativos": total_clientes,
            "multimarcas": total_multimarcas,
            "percent_multimarcas": round(percent_multi, 2),
            "total_itens": int(total_itens),
            "total_valor": float(total_valor),
        })

    # Calculate variations between consecutive cycles
    for i in range(1, len(metricas_por_ciclo)):
        atual = metricas_por_ciclo[i]
        anterior = metricas_por_ciclo[i - 1]

        # Calculate percentage variations
        for key in ["clientes_ativos", "multimarcas", "total_itens", "total_valor"]:
            val_ant = anterior[key]
            val_atu = atual[key]

            if val_ant > 0:
                variacao = ((val_atu - val_ant) / val_ant) * 100
            else:
                variacao = 100 if val_atu > 0 else 0

            atual[f"var_{key}"] = round(variacao, 2)

    return {
        "ciclos": ciclos_ordenados,
        "metricas": metricas_por_ciclo,
        "total_ciclos": len(ciclos_ordenados),
    }
