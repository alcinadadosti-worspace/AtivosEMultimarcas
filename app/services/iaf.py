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

    def add_with_variations(sku: str, descricao: str, marca: str, tipo: str):
        """Add SKU and its variations to index."""
        indice[sku] = {"descricao": descricao, "marca": marca, "tipo": tipo}
        if len(sku) == 5 and sku.startswith('0'):
            indice[sku[1:]] = {"descricao": descricao, "marca": marca, "tipo": tipo}
        if len(sku) == 4:
            indice['0' + sku] = {"descricao": descricao, "marca": marca, "tipo": tipo}

    # Load Cabelos
    cursor.execute("SELECT sku_normalizado, descricao, marca FROM iaf_cabelos")
    for row in cursor.fetchall():
        sku, descricao, marca = row
        add_with_variations(sku, descricao, marca, "Cabelos")

    # Load Make
    cursor.execute("SELECT sku_normalizado, descricao, marca FROM iaf_make")
    for row in cursor.fetchall():
        sku, descricao, marca = row
        add_with_variations(sku, descricao, marca, "Make")

    return indice


def is_siage_hair_product(nome_produto: str) -> bool:
    """
    Check if a product is a Siège hair product (combo/kit/sachet).

    These products should be counted as IAF Cabelos even if not in the official list.
    """
    if not nome_produto:
        return False
    nome_upper = nome_produto.upper()

    # Must contain Siège indicator
    if 'SIAGE' not in nome_upper and 'SIÀGE' not in nome_upper:
        return False

    # Must be a hair-related product (combo, kit, sachet, or hair keywords)
    hair_indicators = [
        'COMB', 'KIT', 'CJ SCH', 'SACHET', 'SCH ',
        'SHAMP', 'COND', 'MASC', 'CREME', 'SERUM', 'LEAVE', 'OLEO'
    ]
    return any(ind in nome_upper for ind in hair_indicators)


def is_makeup_product(nome_produto: str) -> bool:
    """
    Check if a product is a makeup product that should be counted as IAF Make.

    Detects makeup products that may not be in the official IAF list.
    """
    if not nome_produto:
        return False
    nome_upper = nome_produto.upper()

    # Makeup product indicators
    makeup_indicators = [
        'BATOM', 'GLOSS', 'SOMBRA', 'PLT SOMBRA', 'PALETTE SOMBRA',
        'RIMEL', 'MASCARA CILIOS', 'DELINEADOR', 'BLUSH', 'BRONZER',
        'ILUMINADOR', 'PRIMER', 'CORRETIVO', 'PO COMPACTO', 'CONTORNO',
        'BASE LIQ', 'BASE STICK', 'MAKE B ', 'MAKEB ', 'LABIAL',
        'LAPIS OLHO', 'LAPIS BOCA', 'EUD MAKE', 'NIINA'
    ]

    # Check if product name matches makeup indicators
    if any(ind in nome_upper for ind in makeup_indicators):
        # Exclude products that are clearly not makeup
        exclude_patterns = ['BODY', 'CORPORAL', 'CABELO', 'SHAMP', 'COND']
        if not any(excl in nome_upper for excl in exclude_patterns):
            return True

    return False


def cruzar_vendas_com_iaf(
    df_vendas: pl.DataFrame,
    conn: sqlite3.Connection
) -> pl.DataFrame:
    """
    Cross-reference sales with IAF database to identify premium items.

    Also includes Siège hair products (combos/kits) that aren't in the official
    IAF list but should be counted as IAF Cabelos.

    Args:
        df_vendas: Enriched sales DataFrame
        conn: SQLite connection

    Returns:
        DataFrame with only IAF sales, including type (Cabelos/Make)
        Also includes "TipoTransacao" column for filtering Make without Brinde
    """
    # Load IAF index
    indice_iaf = criar_indice_iaf(conn)

    if not indice_iaf:
        return pl.DataFrame()

    # Filter sales that are in IAF
    resultados = []

    for row in df_vendas.iter_rows(named=True):
        codigo = row.get("CodigoProduto_normalizado", "")
        nome_produto = row.get("NomeProduto", "")
        tipo_transacao = row.get("Tipo", "Venda")

        tipo_iaf = None
        descricao = ""
        marca = ""

        # Check if in official IAF index
        if codigo in indice_iaf:
            info = indice_iaf[codigo]
            tipo_iaf = info["tipo"]
            descricao = info["descricao"]
            marca = info["marca"]
        # Check if it's a Siège hair product (combo/kit) not in official list
        elif is_siage_hair_product(nome_produto):
            tipo_iaf = "Cabelos"
            descricao = nome_produto
            marca = "Eudora"
        # Check if it's a makeup product not in official list
        elif is_makeup_product(nome_produto):
            tipo_iaf = "Make"
            descricao = nome_produto
            # Determine brand from product name
            nome_upper = nome_produto.upper() if nome_produto else ""
            if "EUD" in nome_upper or "EUDORA" in nome_upper:
                marca = "Eudora"
            elif "NIINA" in nome_upper:
                marca = "oBoticário"
            elif "QDB" in nome_upper or "QUEM DISSE" in nome_upper:
                marca = "Quem Disse Berenice"
            else:
                marca = "oBoticário"

        if tipo_iaf:
            resultados.append({
                VENDAS_COL_CICLO: row[VENDAS_COL_CICLO],
                VENDAS_COL_SETOR: row[VENDAS_COL_SETOR],
                VENDAS_COL_CODIGO_REVENDEDORA: row[VENDAS_COL_CODIGO_REVENDEDORA],
                VENDAS_COL_NOME_REVENDEDORA: row[VENDAS_COL_NOME_REVENDEDORA],
                "ClienteID": row["ClienteID"],
                "SKU": codigo,
                "Nome_IAF": descricao,
                "Marca_IAF": marca,
                "TipoIAF": tipo_iaf,
                "TipoTransacao": tipo_transacao,
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

    IMPORTANT: For Make, excludes "Brinde" transaction type from calculation.

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
    df_iaf_filtrado = df_iaf
    if tipo_iaf:
        df_iaf_filtrado = df_iaf_filtrado.filter(pl.col("TipoIAF") == tipo_iaf)

    # IMPORTANT: For Make, exclude "Brinde" transactions
    if tipo_iaf == "Make" and "TipoTransacao" in df_iaf_filtrado.columns:
        df_iaf_filtrado = df_iaf_filtrado.filter(pl.col("TipoTransacao") != "Brinde")

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

    # Round to 2 decimal places
    percentual = round(percentual, 2)

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
    # Cabelos: all transaction types
    df_iaf_cabelos = df_iaf.filter(pl.col("TipoIAF") == "Cabelos").group_by(VENDAS_COL_SETOR).agg([
        pl.col("ClienteID").n_unique().alias("ClientesCabelos")
    ])

    # Make: exclude "Brinde" transactions
    df_make_filter = df_iaf.filter(pl.col("TipoIAF") == "Make")
    if "TipoTransacao" in df_iaf.columns:
        df_make_filter = df_make_filter.filter(pl.col("TipoTransacao") != "Brinde")
    df_iaf_make = df_make_filter.group_by(VENDAS_COL_SETOR).agg([
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
