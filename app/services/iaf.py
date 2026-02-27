"""
IAF (Premium/Incentive) tracking service.

This module handles cross-referencing sales with IAF product lists
and calculating penetration metrics.
"""
import sqlite3
from typing import Any, Dict, List, Optional

import polars as pl

from app.config import (
    VENDAS_COL_CICLO,
    VENDAS_COL_SETOR,
    VENDAS_COL_CODIGO_REVENDEDORA,
    VENDAS_COL_NOME_REVENDEDORA,
    VENDAS_COL_QTD_ITENS,
    VENDAS_COL_VALOR,
)
from app.utils.normalizers import normalizar_sku


def criar_indice_iaf(conn: sqlite3.Connection) -> Dict[str, Dict]:
    """
    Create an in-memory index of IAF products.

    Args:
        conn: SQLite connection

    Returns:
        Dict mapping sku_normalizado -> {descricao, marca, tipo}
    """
    cursor = conn.cursor()

    indice = {}

    # Load Cabelos
    cursor.execute("SELECT sku_normalizado, descricao, marca FROM iaf_cabelos")
    for row in cursor.fetchall():
        sku, descricao, marca = row
        indice[sku] = {"descricao": descricao, "marca": marca, "tipo": "Cabelos"}

        # Add variations
        if len(sku) == 5 and sku.startswith('0'):
            indice[sku[1:]] = {"descricao": descricao, "marca": marca, "tipo": "Cabelos"}
        if len(sku) == 4:
            indice['0' + sku] = {"descricao": descricao, "marca": marca, "tipo": "Cabelos"}

    # Load Make
    cursor.execute("SELECT sku_normalizado, descricao, marca FROM iaf_make")
    for row in cursor.fetchall():
        sku, descricao, marca = row
        indice[sku] = {"descricao": descricao, "marca": marca, "tipo": "Make"}

        # Add variations
        if len(sku) == 5 and sku.startswith('0'):
            indice[sku[1:]] = {"descricao": descricao, "marca": marca, "tipo": "Make"}
        if len(sku) == 4:
            indice['0' + sku] = {"descricao": descricao, "marca": marca, "tipo": "Make"}

    return indice


def cruzar_vendas_com_iaf(
    df_vendas: pl.DataFrame,
    conn: sqlite3.Connection
) -> pl.DataFrame:
    """
    Cross-reference sales with IAF database to identify premium items.

    Args:
        df_vendas: Enriched sales DataFrame
        conn: SQLite connection

    Returns:
        DataFrame with only IAF sales, including type (Cabelos/Make)
    """
    # Load IAF index
    indice_iaf = criar_indice_iaf(conn)

    if not indice_iaf:
        return pl.DataFrame()

    # Filter sales that are in IAF
    resultados = []

    for row in df_vendas.iter_rows(named=True):
        codigo = row.get("CodigoProduto_normalizado", "")

        if codigo in indice_iaf:
            info = indice_iaf[codigo]
            resultados.append({
                VENDAS_COL_CICLO: row[VENDAS_COL_CICLO],
                VENDAS_COL_SETOR: row[VENDAS_COL_SETOR],
                VENDAS_COL_CODIGO_REVENDEDORA: row[VENDAS_COL_CODIGO_REVENDEDORA],
                VENDAS_COL_NOME_REVENDEDORA: row[VENDAS_COL_NOME_REVENDEDORA],
                "ClienteID": row["ClienteID"],
                "SKU": codigo,
                "Nome_IAF": info["descricao"],
                "Marca_IAF": info["marca"],
                "TipoIAF": info["tipo"],
                VENDAS_COL_QTD_ITENS: row[VENDAS_COL_QTD_ITENS],
                VENDAS_COL_VALOR: row[VENDAS_COL_VALOR],
            })

    if not resultados:
        return pl.DataFrame()

    return pl.DataFrame(resultados)


def calcular_percentual_iaf(
    df_clientes: pl.DataFrame,
    df_iaf: pl.DataFrame,
    tipo_iaf: Optional[str] = None
) -> Dict[str, Any]:
    """
    Calculate percentage of customers who purchased IAF items.

    SPECIAL RULE FOR MAKE:
    - If the decimal part of percentage is >= 0.5, add +1.5%
    - Example: 23.5% -> 25% (23.5 + 1.5 = 25)

    Args:
        df_clientes: DataFrame with all active customers
        df_iaf: DataFrame of IAF sales
        tipo_iaf: Filter by type ("Cabelos", "Make") or None for all

    Returns:
        Dict with IAF metrics
    """
    total_clientes = df_clientes.select(pl.col("ClienteID").n_unique()).item()

    if df_iaf.is_empty():
        return {
            "total_clientes": total_clientes,
            "clientes_iaf": 0,
            "percentual": 0.0,
            "tipo": tipo_iaf or "Todos"
        }

    # Filter by type if specified
    if tipo_iaf:
        df_iaf_filtrado = df_iaf.filter(pl.col("TipoIAF") == tipo_iaf)
    else:
        df_iaf_filtrado = df_iaf

    if df_iaf_filtrado.is_empty():
        return {
            "total_clientes": total_clientes,
            "clientes_iaf": 0,
            "percentual": 0.0,
            "tipo": tipo_iaf or "Todos"
        }

    clientes_iaf = df_iaf_filtrado.select(pl.col("ClienteID").n_unique()).item()

    # Calculate percentage
    percentual = (clientes_iaf / total_clientes * 100) if total_clientes > 0 else 0

    # SPECIAL MAKE RULE: +1.5% if decimal >= 0.5
    if tipo_iaf == "Make":
        parte_decimal = percentual - int(percentual)
        if parte_decimal >= 0.5:
            percentual += 1.5

    # Round
    percentual = round(percentual)

    return {
        "total_clientes": total_clientes,
        "clientes_iaf": clientes_iaf,
        "percentual": percentual,
        "tipo": tipo_iaf or "Todos"
    }


def calcular_iaf_por_setor(
    df_clientes: pl.DataFrame,
    df_iaf: pl.DataFrame
) -> List[Dict[str, Any]]:
    """
    Calculate IAF metrics by sector.

    Args:
        df_clientes: DataFrame with customer metrics
        df_iaf: DataFrame of IAF sales

    Returns:
        List of dicts with sector IAF metrics
    """
    # Get active customers by sector
    df_setores = df_clientes.group_by(VENDAS_COL_SETOR).agg([
        pl.col("ClienteID").n_unique().alias("ClientesAtivos")
    ])

    if df_iaf.is_empty():
        return [
            {
                "setor": row[VENDAS_COL_SETOR],
                "clientes_ativos": row["ClientesAtivos"],
                "clientes_cabelos": 0,
                "percent_cabelos": 0,
                "clientes_make": 0,
                "percent_make": 0,
            }
            for row in df_setores.iter_rows(named=True)
        ]

    # Count IAF customers by sector and type
    df_iaf_cabelos = df_iaf.filter(pl.col("TipoIAF") == "Cabelos").group_by(VENDAS_COL_SETOR).agg([
        pl.col("ClienteID").n_unique().alias("ClientesCabelos")
    ])

    df_iaf_make = df_iaf.filter(pl.col("TipoIAF") == "Make").group_by(VENDAS_COL_SETOR).agg([
        pl.col("ClienteID").n_unique().alias("ClientesMake")
    ])

    # Join all data
    df_resultado = df_setores.join(df_iaf_cabelos, on=VENDAS_COL_SETOR, how="left")
    df_resultado = df_resultado.join(df_iaf_make, on=VENDAS_COL_SETOR, how="left")

    # Fill nulls and calculate percentages
    df_resultado = df_resultado.with_columns([
        pl.col("ClientesCabelos").fill_null(0),
        pl.col("ClientesMake").fill_null(0),
    ])

    resultados = []
    for row in df_resultado.iter_rows(named=True):
        clientes_ativos = row["ClientesAtivos"]
        clientes_cabelos = row["ClientesCabelos"]
        clientes_make = row["ClientesMake"]

        percent_cabelos = round((clientes_cabelos / clientes_ativos * 100)) if clientes_ativos > 0 else 0
        percent_make = round((clientes_make / clientes_ativos * 100)) if clientes_ativos > 0 else 0

        resultados.append({
            "setor": row[VENDAS_COL_SETOR],
            "clientes_ativos": clientes_ativos,
            "clientes_cabelos": clientes_cabelos,
            "percent_cabelos": percent_cabelos,
            "clientes_make": clientes_make,
            "percent_make": percent_make,
        })

    return sorted(resultados, key=lambda x: x["setor"])


def listar_vendas_iaf(
    df_iaf: pl.DataFrame,
    tipo_iaf: Optional[str] = None,
    setor: Optional[str] = None,
    limite: int = 100
) -> List[Dict[str, Any]]:
    """
    List IAF sales with optional filters.

    Args:
        df_iaf: DataFrame of IAF sales
        tipo_iaf: Filter by type ("Cabelos", "Make")
        setor: Filter by sector
        limite: Maximum number of results

    Returns:
        List of IAF sale records
    """
    if df_iaf.is_empty():
        return []

    df_filtrado = df_iaf

    if tipo_iaf:
        df_filtrado = df_filtrado.filter(pl.col("TipoIAF") == tipo_iaf)

    if setor:
        df_filtrado = df_filtrado.filter(pl.col(VENDAS_COL_SETOR).cast(pl.Utf8) == setor)

    df_filtrado = df_filtrado.head(limite)

    return [
        {
            "ciclo": row[VENDAS_COL_CICLO],
            "setor": row[VENDAS_COL_SETOR],
            "codigo_revendedora": row[VENDAS_COL_CODIGO_REVENDEDORA],
            "nome_revendedora": row[VENDAS_COL_NOME_REVENDEDORA],
            "sku": row["SKU"],
            "nome": row["Nome_IAF"],
            "marca": row["Marca_IAF"],
            "tipo": row["TipoIAF"],
            "quantidade": row[VENDAS_COL_QTD_ITENS],
            "valor": row[VENDAS_COL_VALOR],
        }
        for row in df_filtrado.iter_rows(named=True)
    ]
