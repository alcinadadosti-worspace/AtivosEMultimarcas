"""
Metrics calculation service.

This module provides functions to calculate various business metrics
from sales data, including multi-brand customer analysis.
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
    MARCA_DESCONHECIDA,
)


def calcular_metricas_cliente(df_vendas: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate aggregated metrics per customer and cycle.

    Identifies:
    - Active customers (with at least 1 sale in the cycle)
    - Number of distinct brands per customer/cycle
    - Multi-brand flag (2+ known brands)
    - List of purchased brands

    Args:
        df_vendas: DataFrame filtered to Tipo="Venda" only

    Returns:
        DataFrame with one row per customer/cycle containing:
        - CicloFaturamento, ClienteID, Setor, CodigoRevendedora, NomeRevendedora
        - MarcasCompradas (comma-separated list)
        - QtdeMarcasDistintas (brand count)
        - IsMultimarcas (True if 2+ brands)
        - ItensTotal, ValorTotal
    """
    # Group by customer and cycle
    df_clientes = df_vendas.group_by([
        VENDAS_COL_CICLO,
        "ClienteID",
        VENDAS_COL_SETOR,
        VENDAS_COL_CODIGO_REVENDEDORA,
        VENDAS_COL_NOME_REVENDEDORA
    ]).agg([
        # List of unique brands (excluding DESCONHECIDA)
        pl.col("Marca_BD")
          .filter(pl.col("Marca_BD") != MARCA_DESCONHECIDA)
          .unique()
          .sort()
          .str.concat(", ")
          .alias("MarcasCompradas"),

        # Number of distinct brands
        pl.col("Marca_BD")
          .filter(pl.col("Marca_BD") != MARCA_DESCONHECIDA)
          .n_unique()
          .alias("QtdeMarcasDistintas"),

        # Totals
        pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum().alias("ItensTotal"),
        pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum().alias("ValorTotal"),
    ])

    # Multi-brand flag (2+ known brands)
    df_clientes = df_clientes.with_columns([
        (pl.col("QtdeMarcasDistintas") >= 2).alias("IsMultimarcas")
    ])

    return df_clientes


def calcular_metricas_setor_ciclo(df_clientes: pl.DataFrame) -> pl.DataFrame:
    """
    Aggregate metrics by sector and cycle.

    Args:
        df_clientes: DataFrame with customer metrics

    Returns:
        DataFrame with metrics per sector/cycle:
        - ClientesAtivos: unique customer count
        - ClientesMultimarcas: count with 2+ brands
        - %Multimarcas: multi-brand percentage
        - ItensTotal, ValorTotal: sector totals
    """
    df_setor = df_clientes.group_by([
        VENDAS_COL_CICLO,
        VENDAS_COL_SETOR
    ]).agg([
        pl.col("ClienteID").n_unique().alias("ClientesAtivos"),
        pl.col("IsMultimarcas").sum().alias("ClientesMultimarcas"),
        pl.col("ItensTotal").sum().alias("ItensTotal"),
        pl.col("ValorTotal").sum().alias("ValorTotal"),
    ])

    # Calculate multi-brand percentage
    df_setor = df_setor.with_columns([
        pl.when(pl.col("ClientesAtivos") > 0)
          .then((pl.col("ClientesMultimarcas") / pl.col("ClientesAtivos") * 100).round(0))
          .otherwise(0.0)
          .alias("PercentMultimarcas")
    ])

    return df_setor.sort([VENDAS_COL_CICLO, VENDAS_COL_SETOR])


def calcular_metricas_gerais(
    df_clientes: pl.DataFrame,
    df_vendas: pl.DataFrame
) -> Dict[str, Any]:
    """
    Calculate general metrics for dashboard cards.

    Returns:
        Dict with:
        - total_ativos: unique customer count
        - total_multimarcas: customers with 2+ brands
        - percent_multimarcas: multi-brand percentage
        - total_itens: sum of items sold
        - total_valor: sum of sales value
    """
    total_ativos = df_clientes.select(pl.col("ClienteID").n_unique()).item()
    total_multimarcas = df_clientes.filter(pl.col("IsMultimarcas")).select(pl.col("ClienteID").n_unique()).item()

    percent_multimarcas = (total_multimarcas / total_ativos * 100) if total_ativos > 0 else 0
    percent_multimarcas = round(percent_multimarcas)  # Round to integer

    total_itens = df_vendas.select(pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum()).item()
    total_valor = df_vendas.select(pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum()).item()

    return {
        "total_ativos": total_ativos,
        "total_multimarcas": total_multimarcas,
        "percent_multimarcas": percent_multimarcas,
        "total_itens": int(total_itens or 0),
        "total_valor": float(total_valor or 0),
    }


def calcular_vendas_por_marca(df_vendas: pl.DataFrame) -> List[Dict[str, Any]]:
    """
    Calculate sales totals by brand.

    Args:
        df_vendas: DataFrame with enriched sales data

    Returns:
        List of dicts with brand name, item count, and value
    """
    df_marcas = df_vendas.group_by("Marca_BD").agg([
        pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum().alias("ItensTotal"),
        pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum().alias("ValorTotal"),
        pl.count().alias("QtdeVendas"),
    ]).sort("ValorTotal", descending=True)

    return [
        {
            "marca": row["Marca_BD"],
            "itens": int(row["ItensTotal"] or 0),
            "valor": float(row["ValorTotal"] or 0),
            "vendas": row["QtdeVendas"],
        }
        for row in df_marcas.iter_rows(named=True)
    ]


def calcular_top_setores(
    df_clientes: pl.DataFrame,
    limite: int = 5
) -> List[Dict[str, Any]]:
    """
    Calculate top sectors by sales value.

    Args:
        df_clientes: DataFrame with customer metrics
        limite: Number of top sectors to return

    Returns:
        List of dicts with sector name and totals
    """
    df_setores = df_clientes.group_by(VENDAS_COL_SETOR).agg([
        pl.col("ClienteID").n_unique().alias("ClientesAtivos"),
        pl.col("IsMultimarcas").sum().alias("ClientesMultimarcas"),
        pl.col("ValorTotal").sum().alias("ValorTotal"),
    ]).sort("ValorTotal", descending=True).head(limite)

    return [
        {
            "setor": row[VENDAS_COL_SETOR],
            "clientes": row["ClientesAtivos"],
            "multimarcas": row["ClientesMultimarcas"],
            "valor": float(row["ValorTotal"] or 0),
        }
        for row in df_setores.iter_rows(named=True)
    ]


def calcular_evolucao_ciclos(df_clientes: pl.DataFrame) -> List[Dict[str, Any]]:
    """
    Calculate metrics evolution by cycle.

    Args:
        df_clientes: DataFrame with customer metrics

    Returns:
        List of dicts with cycle and metrics
    """
    df_ciclos = df_clientes.group_by(VENDAS_COL_CICLO).agg([
        pl.col("ClienteID").n_unique().alias("ClientesAtivos"),
        pl.col("IsMultimarcas").sum().alias("ClientesMultimarcas"),
        pl.col("ValorTotal").sum().alias("ValorTotal"),
    ]).sort(VENDAS_COL_CICLO)

    # Calculate percentage
    df_ciclos = df_ciclos.with_columns([
        pl.when(pl.col("ClientesAtivos") > 0)
          .then((pl.col("ClientesMultimarcas") / pl.col("ClientesAtivos") * 100).round(1))
          .otherwise(0.0)
          .alias("PercentMultimarcas")
    ])

    return [
        {
            "ciclo": row[VENDAS_COL_CICLO],
            "clientes": row["ClientesAtivos"],
            "multimarcas": row["ClientesMultimarcas"],
            "percent": float(row["PercentMultimarcas"]),
            "valor": float(row["ValorTotal"] or 0),
        }
        for row in df_ciclos.iter_rows(named=True)
    ]


def aplicar_filtros(
    df: pl.DataFrame,
    ciclos: Optional[List[str]] = None,
    setores: Optional[List[str]] = None,
    marcas: Optional[List[str]] = None,
    gerencias: Optional[List[str]] = None,
    apenas_multimarcas: bool = False
) -> pl.DataFrame:
    """
    Apply filters to DataFrame.

    Args:
        df: DataFrame to filter
        ciclos: List of cycles to filter by
        setores: List of sectors to filter by
        marcas: List of brands to filter by
        gerencias: List of management codes (partial search)
        apenas_multimarcas: If True, return only multi-brand customers

    Returns:
        Filtered DataFrame
    """
    df_filtrado = df

    if ciclos and len(ciclos) > 0:
        df_filtrado = df_filtrado.filter(pl.col(VENDAS_COL_CICLO).is_in(ciclos))

    if setores and len(setores) > 0:
        df_filtrado = df_filtrado.filter(pl.col(VENDAS_COL_SETOR).cast(pl.Utf8).is_in(setores))

    if marcas and len(marcas) > 0 and "Marca_BD" in df_filtrado.columns:
        df_filtrado = df_filtrado.filter(pl.col("Marca_BD").is_in(marcas))

    if gerencias and len(gerencias) > 0 and VENDAS_COL_GERENCIA in df_filtrado.columns:
        # Partial search - find management containing the code
        pattern = "|".join(gerencias)
        df_filtrado = df_filtrado.filter(
            pl.col(VENDAS_COL_GERENCIA).cast(pl.Utf8).str.contains(pattern)
        )

    if apenas_multimarcas and "IsMultimarcas" in df_filtrado.columns:
        df_filtrado = df_filtrado.filter(pl.col("IsMultimarcas"))

    return df_filtrado


def obter_detalhes_cliente(
    df_vendas: pl.DataFrame,
    cliente_id: str
) -> Dict[str, Any]:
    """
    Get detailed information for a specific customer.

    Args:
        df_vendas: DataFrame with enriched sales data
        cliente_id: Customer ID to look up

    Returns:
        Dict with customer info and purchase history
    """
    df_cliente = df_vendas.filter(pl.col("ClienteID") == cliente_id)

    if df_cliente.is_empty():
        return {"encontrado": False}

    # Get customer basic info
    primeiro = df_cliente.row(0, named=True)
    nome = primeiro.get(VENDAS_COL_NOME_REVENDEDORA, "")
    codigo = primeiro.get(VENDAS_COL_CODIGO_REVENDEDORA, "")
    setor = primeiro.get(VENDAS_COL_SETOR, "")

    # Calculate totals
    total_itens = df_cliente.select(pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum()).item()
    total_valor = df_cliente.select(pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum()).item()

    # Get unique brands
    marcas = df_cliente.filter(
        pl.col("Marca_BD") != MARCA_DESCONHECIDA
    ).select(pl.col("Marca_BD").unique()).to_series().to_list()

    # Get purchase history
    compras = [
        {
            "ciclo": row[VENDAS_COL_CICLO],
            "setor": row[VENDAS_COL_SETOR],
            "codigo_produto": row["CodigoProduto_normalizado"],
            "nome_produto": row["Nome_BD"],
            "marca": row["Marca_BD"],
            "quantidade": row[VENDAS_COL_QTD_ITENS],
            "valor": row[VENDAS_COL_VALOR],
        }
        for row in df_cliente.iter_rows(named=True)
    ]

    return {
        "encontrado": True,
        "cliente_id": cliente_id,
        "nome": nome,
        "codigo": codigo,
        "setor": setor,
        "total_itens": int(total_itens or 0),
        "total_valor": float(total_valor or 0),
        "marcas": sorted(marcas),
        "qtde_marcas": len(marcas),
        "is_multimarcas": len(marcas) >= 2,
        "compras": compras,
    }
