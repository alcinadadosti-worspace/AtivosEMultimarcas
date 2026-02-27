"""
Audit service for SKU matching issues.

This module provides functions to identify and report SKUs with
matching problems or that are not in the product database.
"""
from typing import Any, Dict, List

import polars as pl

from app.config import (
    VENDAS_COL_CICLO,
    VENDAS_COL_SETOR,
    VENDAS_COL_CODIGO_REVENDEDORA,
    VENDAS_COL_CODIGO_PRODUTO,
    VENDAS_COL_NOME_PRODUTO,
    VENDAS_COL_QTD_ITENS,
    VENDAS_COL_VALOR,
    MOTIVO_NAO_ENCONTRADO,
    MOTIVO_MATCH_COM_ZERO,
    MOTIVO_MATCH_SEM_ZERO,
)


def gerar_auditoria_skus(df_vendas: pl.DataFrame) -> pl.DataFrame:
    """
    Generate audit table for SKUs with matching problems.

    Includes:
    - SKUs not found in database
    - SKUs found with special match (with/without zero)

    Useful for identifying:
    - Products that need to be registered
    - Typos in product codes
    - Formatting issues

    Args:
        df_vendas: Enriched sales DataFrame

    Returns:
        DataFrame with audit records
    """
    # Filter rows with problems or special match
    df_audit = df_vendas.filter(
        (pl.col("Motivo_Match") == MOTIVO_NAO_ENCONTRADO) |
        (pl.col("Motivo_Match") == MOTIVO_MATCH_COM_ZERO) |
        (pl.col("Motivo_Match") == MOTIVO_MATCH_SEM_ZERO)
    )

    # Select relevant columns and remove duplicates
    df_audit = df_audit.select([
        VENDAS_COL_CICLO,
        VENDAS_COL_SETOR,
        VENDAS_COL_CODIGO_REVENDEDORA,
        VENDAS_COL_CODIGO_PRODUTO,
        "CodigoProduto_normalizado",
        VENDAS_COL_NOME_PRODUTO,
        "Motivo_Match"
    ]).unique()

    return df_audit


def gerar_produtos_nao_cadastrados(df_vendas: pl.DataFrame) -> pl.DataFrame:
    """
    Generate table of products not registered (possible new releases).

    Aggregates by SKU showing:
    - Number of sales
    - Total items
    - Total value
    - Cycles and sectors where it appeared

    Sorted by total value (most relevant first).

    Args:
        df_vendas: Enriched sales DataFrame

    Returns:
        DataFrame with unregistered products
    """
    # Filter only not found
    df_novos = df_vendas.filter(pl.col("Motivo_Match") == MOTIVO_NAO_ENCONTRADO)

    if df_novos.is_empty():
        return pl.DataFrame()

    # Aggregate by SKU
    df_agg = df_novos.group_by([
        "CodigoProduto_normalizado",
        VENDAS_COL_NOME_PRODUTO
    ]).agg([
        pl.count().alias("Qtde_Vendas"),
        pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum().alias("Total_Itens"),
        pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum().alias("Valor_Total"),
        pl.col(VENDAS_COL_CICLO).unique().sort().str.concat(", ").alias("Ciclos"),
        pl.col(VENDAS_COL_SETOR).cast(pl.Utf8).unique().sort().str.concat(", ").alias("Setores"),
    ])

    # Sort by value (most relevant first)
    df_agg = df_agg.sort("Valor_Total", descending=True)

    # Rename columns
    df_agg = df_agg.rename({
        "CodigoProduto_normalizado": "SKU",
        VENDAS_COL_NOME_PRODUTO: "Nome_Produto"
    })

    return df_agg


def obter_estatisticas_auditoria(df_vendas: pl.DataFrame) -> Dict[str, Any]:
    """
    Get audit statistics summary.

    Args:
        df_vendas: Enriched sales DataFrame

    Returns:
        Dict with audit statistics
    """
    total_vendas = len(df_vendas)

    nao_encontrados = len(df_vendas.filter(pl.col("Motivo_Match") == MOTIVO_NAO_ENCONTRADO))
    match_com_zero = len(df_vendas.filter(pl.col("Motivo_Match") == MOTIVO_MATCH_COM_ZERO))
    match_sem_zero = len(df_vendas.filter(pl.col("Motivo_Match") == MOTIVO_MATCH_SEM_ZERO))
    match_exato = total_vendas - nao_encontrados - match_com_zero - match_sem_zero

    # Unique SKUs
    skus_nao_encontrados = df_vendas.filter(
        pl.col("Motivo_Match") == MOTIVO_NAO_ENCONTRADO
    ).select(pl.col("CodigoProduto_normalizado").n_unique()).item()

    return {
        "total_vendas": total_vendas,
        "match_exato": match_exato,
        "match_com_zero": match_com_zero,
        "match_sem_zero": match_sem_zero,
        "nao_encontrados": nao_encontrados,
        "skus_unicos_nao_encontrados": skus_nao_encontrados,
        "taxa_match": round((total_vendas - nao_encontrados) / total_vendas * 100, 1) if total_vendas > 0 else 0,
    }


def listar_auditoria(
    df_vendas: pl.DataFrame,
    motivo: str = None,
    limite: int = 100
) -> List[Dict[str, Any]]:
    """
    List audit records with optional filtering.

    Args:
        df_vendas: Enriched sales DataFrame
        motivo: Filter by match reason
        limite: Maximum number of results

    Returns:
        List of audit records
    """
    df_audit = gerar_auditoria_skus(df_vendas)

    if df_audit.is_empty():
        return []

    if motivo:
        df_audit = df_audit.filter(pl.col("Motivo_Match") == motivo)

    df_audit = df_audit.head(limite)

    return [
        {
            "ciclo": row[VENDAS_COL_CICLO],
            "setor": row[VENDAS_COL_SETOR],
            "codigo_revendedora": row[VENDAS_COL_CODIGO_REVENDEDORA],
            "codigo_produto_original": row[VENDAS_COL_CODIGO_PRODUTO],
            "codigo_normalizado": row["CodigoProduto_normalizado"],
            "nome_produto": row[VENDAS_COL_NOME_PRODUTO],
            "motivo": row["Motivo_Match"],
        }
        for row in df_audit.iter_rows(named=True)
    ]


def listar_produtos_novos(
    df_vendas: pl.DataFrame,
    limite: int = 100
) -> List[Dict[str, Any]]:
    """
    List unregistered products (potential new releases).

    Args:
        df_vendas: Enriched sales DataFrame
        limite: Maximum number of results

    Returns:
        List of unregistered products sorted by value
    """
    df_novos = gerar_produtos_nao_cadastrados(df_vendas)

    if df_novos.is_empty():
        return []

    df_novos = df_novos.head(limite)

    return [
        {
            "sku": row["SKU"],
            "nome": row["Nome_Produto"],
            "qtde_vendas": row["Qtde_Vendas"],
            "total_itens": int(row["Total_Itens"] or 0),
            "valor_total": float(row["Valor_Total"] or 0),
            "ciclos": row["Ciclos"],
            "setores": row["Setores"],
        }
        for row in df_novos.iter_rows(named=True)
    ]
