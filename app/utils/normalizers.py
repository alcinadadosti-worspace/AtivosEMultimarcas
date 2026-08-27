"""
Normalization utilities for SKU codes, brand names and city names.

Critical functions for ensuring consistent data matching across sales
spreadsheets and product database.
"""
import re
import math
import unicodedata
from typing import Any

import polars as pl

from app.config import MARCA_ALIASES, MARCA_DESCONHECIDA


def normalizar_sku(valor: Any) -> str:
    """
    Normalize a SKU/ProductCode value to a consistent format.

    CRITICAL RULES:
    1. Convert to string
    2. Remove whitespace (start, end, and middle)
    3. Remove non-numeric characters (letters, symbols)
    4. PRESERVE leading zeros (DO NOT convert to int)
    5. Handle floats correctly (1234.0 -> "1234")
    6. Return empty string if invalid

    Args:
        valor: Any value (str, int, float, None)

    Returns:
        String containing only digits, preserving leading zeros

    Examples:
        >>> normalizar_sku("01234")
        "01234"
        >>> normalizar_sku(1234)
        "1234"
        >>> normalizar_sku(1234.0)
        "1234"
        >>> normalizar_sku("  01234  ")
        "01234"
        >>> normalizar_sku("ABC123")
        "123"
        >>> normalizar_sku(None)
        ""
        >>> normalizar_sku(float('nan'))
        ""
    """
    # Handle None
    if valor is None:
        return ""

    # Handle float NaN
    if isinstance(valor, float):
        if math.isnan(valor):
            return ""
        # If it's an integer float (e.g., 1234.0), convert to int first
        if valor == int(valor):
            valor = int(valor)

    # Convert to string
    valor_str = str(valor).strip()

    # Remove .0 suffix if present (common when Excel reads as float)
    if valor_str.endswith('.0'):
        valor_str = valor_str[:-2]

    # Keep only digits (removes letters, spaces, symbols)
    apenas_digitos = re.sub(r'[^0-9]', '', valor_str)

    return apenas_digitos


def normalizar_marca(marca: Any) -> str:
    """
    Normalize brand name to standardized format.

    Args:
        marca: Brand name (may contain spelling variations)

    Returns:
        Standardized brand name or "DESCONHECIDA"

    Examples:
        >>> normalizar_marca("EUD")
        "Eudora"
        >>> normalizar_marca("OBOTICARIO")
        "oBoticário"
        >>> normalizar_marca(None)
        "DESCONHECIDA"
    """
    if marca is None or (isinstance(marca, float) and math.isnan(marca)):
        return MARCA_DESCONHECIDA

    marca_str = str(marca).strip()

    if not marca_str:
        return MARCA_DESCONHECIDA

    # Try match in aliases dictionary (case-insensitive)
    marca_upper = marca_str.upper()

    if marca_upper in MARCA_ALIASES:
        return MARCA_ALIASES[marca_upper]

    # If no alias found, return original (stripped)
    return marca_str.strip()


def chave_cidade(valor: Any) -> str:
    """
    Chave canônica de município: só letras e números, sem acento.

    As planilhas escrevem o mesmo lugar de jeitos diferentes e o IBGE de um
    terceiro. Sem uma chave comum o município vira duas cidades no ranking e
    uma delas não acha o polígono do mapa.

    Examples:
        >>> chave_cidade("OLHO D'ÁGUA GRANDE")
        "OLHODAGUAGRANDE"
        >>> chave_cidade("OLHO DÁGUA GRANDE")
        "OLHODAGUAGRANDE"
        >>> chave_cidade("Olho d'Água Grande")
        "OLHODAGUAGRANDE"
        >>> chave_cidade(None)
        ""
    """
    if valor is None or (isinstance(valor, float) and math.isnan(valor)):
        return ""
    s = unicodedata.normalize("NFD", str(valor).strip().upper())
    return "".join(c for c in s if c.isalnum())


def canonizar_cidade(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Faz as grafias do mesmo município convergirem para uma só em ``col``.

    Vence a grafia mais frequente do arquivo (empate: ordem alfabética), então
    a que sobra é a que o pessoal mais usa — e é ela que vai ao mapa e à
    tabela. Sem isto, "OLHO D'ÁGUA GRANDE" e "OLHO DÁGUA GRANDE" viram duas
    linhas e só a primeira casa com o polígono do IBGE.
    """
    if df.is_empty() or col not in df.columns:
        return df

    chave = pl.col(col).map_elements(chave_cidade, return_dtype=pl.Utf8)
    canon = (
        df.select(col)
        .with_columns(chave.alias("_chave"))
        .group_by(["_chave", col])
        .agg(pl.len().alias("_n"))
        .group_by("_chave")
        .agg(
            pl.col(col)
            .sort_by(["_n", col], descending=[True, False])
            .first()
            .alias("_canon")
        )
    )
    return (
        df.with_columns(chave.alias("_chave"))
        .join(canon, on="_chave", how="left")
        .with_columns(pl.coalesce([pl.col("_canon"), pl.col(col)]).alias(col))
        .drop(["_chave", "_canon"])
    )
