"""
Normalization utilities for SKU codes and brand names.

Critical functions for ensuring consistent data matching across sales
spreadsheets and product database.
"""
import re
import math
from typing import Any

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
