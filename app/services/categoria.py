"""
Category classification service.

This module categorizes products based on keywords in product names.
"""
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from app.config import (
    VENDAS_COL_CICLO,
    VENDAS_COL_SETOR,
    VENDAS_COL_QTD_ITENS,
    VENDAS_COL_VALOR,
)


# Category definitions with keywords (case-insensitive matching)
# Order matters - first match wins, so more specific categories should come first
CATEGORIAS = {
    "Demonstradores": [
        "DEM ", "DEMON", "DEMONSTRAD", "DEMONSTRADOR", " CJ ", "CJ ", " FLAC", "FLAC "
    ],
    "Cabelos": [
        "SIAGE", "SIÀGE", "MATCH"
    ],
    "Maquiagem": [
        "GLAM", "PO COMP", " PO ", "CORR LIQ", " CORR ", "MASC CILIO", " MASC ",
        "BASE LIQ", "BASE STICK", " BASE ", " BAS ", "GLOSS", " GLOS ",
        "BLUSH LIQ", " BLUSH ", "BAT LIQ", " BAT ", " SOUL ", "BALM",
        "GLIT", "OIL SHIN", "PLT MULTIF", " PLT ", "CORRET", "LAP OLH",
        " ILUM ", "PRIMER", "SOMBRA", " SOMB ", "SOBRANC", " MAKE ",
        "FAC STICK", "HID LAB", "BATOM"
    ],
    "Perfumaria": [
        " COL ", " EDP ", "EDP ", " COL"
    ],
    "Barba": [
        "BARB", "BARBA"
    ],
    "Acessorios": [
        "PINCEL", "PINCEIS", "NECESS", "NECESSAIRE", "PALETA", "MASSAG",
        "MASSAGEADOR", "APONTADOR", "ESPONJA", "ESPNJ", "FRASQUEIRA",
        "VAPORIZADOR", "MALETA", "TOALHA", " CASE ", "BOLSA", "CURVADOR",
        " CLIP ", "PORTA ", "ESPELHO", "LENCO", " LUVA"
    ],
    "Cuidados com a Pele": [
        " CPO ", "CORPORAL", " MAO ", " MAOS ", " HID ", "INSTANCE CR"
    ],
    "Cuidados Faciais": [
        " FAC ", "NEO DERMO", "NEO D", " SKIN ", "SKINQ", "FACIAL"
    ],
    "Desodorantes": [
        " DES ", "ROLL ON", " AER ", "AEROSSOL", "ANTIT", " ANT ",
        " SPR ", "BDY SPR"
    ],
    "Embalagens": [
        "SACOLA", "KIT TAG", " TAG "
    ],
    "Gifts": [
        "PMPCK", " ESTJ ", " KIT "
    ],
    "Sabonete Corpo": [
        "ESF CPO", "SAB BARR", " SAB ", " SHW ", "SHW GEL"
    ],
    "Solar": [
        " SOL ", " PR ", " PROT ", "PROT "
    ],
    "Unhas": [
        "ESMLT", "ESMALTE"
    ],
    "Oleos": [
        " OL ", "OLEO", "ÓLEO"
    ],
}

# Default category for items that don't match any keywords
CATEGORIA_OUTROS = "Outros"


def classificar_produto(nome_produto: str) -> str:
    """
    Classify a product into a category based on its name.

    Args:
        nome_produto: Product name to classify

    Returns:
        Category name
    """
    if not nome_produto:
        return CATEGORIA_OUTROS

    nome_upper = f" {nome_produto.upper()} "  # Add spaces for word boundary matching

    # Check each category in order
    for categoria, keywords in CATEGORIAS.items():
        for keyword in keywords:
            if keyword.upper() in nome_upper:
                return categoria

    return CATEGORIA_OUTROS


def classificar_vendas(df_vendas: pl.DataFrame) -> pl.DataFrame:
    """
    Add category column to sales DataFrame.

    Args:
        df_vendas: Sales DataFrame with NomeProduto column

    Returns:
        DataFrame with added Categoria column
    """
    if "NomeProduto" not in df_vendas.columns:
        return df_vendas.with_columns([
            pl.lit(CATEGORIA_OUTROS).alias("Categoria")
        ])

    # Apply classification to each product
    categorias = [
        classificar_produto(nome)
        for nome in df_vendas["NomeProduto"].to_list()
    ]

    return df_vendas.with_columns([
        pl.Series("Categoria", categorias)
    ])


def calcular_metricas_categoria(df_vendas: pl.DataFrame) -> List[Dict[str, Any]]:
    """
    Calculate metrics by category.

    Args:
        df_vendas: Sales DataFrame (should already have Categoria column)

    Returns:
        List of dicts with category metrics
    """
    # Ensure Categoria column exists
    if "Categoria" not in df_vendas.columns:
        df_vendas = classificar_vendas(df_vendas)

    # Aggregate by category
    df_cat = df_vendas.group_by("Categoria").agg([
        pl.count().alias("QtdeVendas"),
        pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum().alias("QtdeItens"),
        pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum().alias("ValorTotal"),
        pl.col("CodigoProduto_normalizado").n_unique().alias("ProdutosUnicos"),
    ]).sort("ValorTotal", descending=True)

    # Calculate totals for percentages
    total_valor = df_cat.select(pl.col("ValorTotal").sum()).item() or 0
    total_itens = df_cat.select(pl.col("QtdeItens").sum()).item() or 0

    return [
        {
            "categoria": row["Categoria"],
            "qtde_vendas": row["QtdeVendas"],
            "qtde_itens": int(row["QtdeItens"] or 0),
            "valor_total": float(row["ValorTotal"] or 0),
            "produtos_unicos": row["ProdutosUnicos"],
            "percent_valor": round((row["ValorTotal"] / total_valor * 100), 2) if total_valor > 0 else 0,
            "percent_itens": round((row["QtdeItens"] / total_itens * 100), 2) if total_itens > 0 else 0,
        }
        for row in df_cat.iter_rows(named=True)
    ]


def calcular_categoria_por_ciclo(df_vendas: pl.DataFrame) -> List[Dict[str, Any]]:
    """
    Calculate category metrics by cycle.

    Args:
        df_vendas: Sales DataFrame

    Returns:
        List of dicts with category/cycle metrics
    """
    if "Categoria" not in df_vendas.columns:
        df_vendas = classificar_vendas(df_vendas)

    df_cat_ciclo = df_vendas.group_by([VENDAS_COL_CICLO, "Categoria"]).agg([
        pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum().alias("QtdeItens"),
        pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum().alias("ValorTotal"),
    ]).sort([VENDAS_COL_CICLO, "ValorTotal"], descending=[False, True])

    return [
        {
            "ciclo": row[VENDAS_COL_CICLO],
            "categoria": row["Categoria"],
            "qtde_itens": int(row["QtdeItens"] or 0),
            "valor_total": float(row["ValorTotal"] or 0),
        }
        for row in df_cat_ciclo.iter_rows(named=True)
    ]


def calcular_categoria_por_setor(df_vendas: pl.DataFrame) -> List[Dict[str, Any]]:
    """
    Calculate category metrics by sector.

    Args:
        df_vendas: Sales DataFrame

    Returns:
        List of dicts with category/sector metrics
    """
    if "Categoria" not in df_vendas.columns:
        df_vendas = classificar_vendas(df_vendas)

    df_cat_setor = df_vendas.group_by([VENDAS_COL_SETOR, "Categoria"]).agg([
        pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum().alias("QtdeItens"),
        pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum().alias("ValorTotal"),
    ]).sort([VENDAS_COL_SETOR, "ValorTotal"], descending=[False, True])

    return [
        {
            "setor": row[VENDAS_COL_SETOR],
            "categoria": row["Categoria"],
            "qtde_itens": int(row["QtdeItens"] or 0),
            "valor_total": float(row["ValorTotal"] or 0),
        }
        for row in df_cat_setor.iter_rows(named=True)
    ]


def listar_produtos_categoria(
    df_vendas: pl.DataFrame,
    categoria: str,
    limite: int = 50
) -> List[Dict[str, Any]]:
    """
    List products in a specific category.

    Args:
        df_vendas: Sales DataFrame
        categoria: Category to filter
        limite: Maximum products to return

    Returns:
        List of products in the category
    """
    if "Categoria" not in df_vendas.columns:
        df_vendas = classificar_vendas(df_vendas)

    df_cat = df_vendas.filter(pl.col("Categoria") == categoria)

    # Aggregate by product
    df_produtos = df_cat.group_by(["CodigoProduto_normalizado", "NomeProduto"]).agg([
        pl.col(VENDAS_COL_QTD_ITENS).cast(pl.Float64).sum().alias("QtdeItens"),
        pl.col(VENDAS_COL_VALOR).cast(pl.Float64).sum().alias("ValorTotal"),
    ]).sort("ValorTotal", descending=True).head(limite)

    return [
        {
            "sku": row["CodigoProduto_normalizado"],
            "nome": row["NomeProduto"],
            "qtde_itens": int(row["QtdeItens"] or 0),
            "valor_total": float(row["ValorTotal"] or 0),
        }
        for row in df_produtos.iter_rows(named=True)
    ]


def obter_categorias_disponiveis() -> List[str]:
    """
    Get list of available categories.

    Returns:
        List of category names
    """
    return list(CATEGORIAS.keys()) + [CATEGORIA_OUTROS]
