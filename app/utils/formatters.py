"""
Formatting utilities for display values.
"""
from typing import Union


def formatar_moeda(valor: Union[float, int, None], simbolo: str = "R$") -> str:
    """
    Format a numeric value as Brazilian currency.

    Args:
        valor: Numeric value to format
        simbolo: Currency symbol (default: R$)

    Returns:
        Formatted currency string

    Examples:
        >>> formatar_moeda(1234.56)
        "R$ 1.234,56"
        >>> formatar_moeda(1000000)
        "R$ 1.000.000,00"
    """
    if valor is None:
        return f"{simbolo} 0,00"

    # Format with thousand separators and 2 decimal places
    # Use Brazilian format: . for thousands, , for decimals
    valor_abs = abs(float(valor))
    parte_inteira = int(valor_abs)
    parte_decimal = int(round((valor_abs - parte_inteira) * 100))

    # Format integer part with thousand separators
    parte_inteira_str = f"{parte_inteira:,}".replace(",", ".")

    # Combine with decimal part
    resultado = f"{simbolo} {parte_inteira_str},{parte_decimal:02d}"

    # Add negative sign if needed
    if valor < 0:
        resultado = f"-{resultado}"

    return resultado


def formatar_numero(valor: Union[float, int, None], casas_decimais: int = 0) -> str:
    """
    Format a numeric value with thousand separators.

    Args:
        valor: Numeric value to format
        casas_decimais: Number of decimal places (default: 0)

    Returns:
        Formatted number string

    Examples:
        >>> formatar_numero(1234567)
        "1.234.567"
        >>> formatar_numero(1234.567, 2)
        "1.234,57"
    """
    if valor is None:
        return "0"

    valor_float = float(valor)

    if casas_decimais == 0:
        parte_inteira = int(round(valor_float))
        return f"{parte_inteira:,}".replace(",", ".")
    else:
        # Round to specified decimal places
        valor_arredondado = round(valor_float, casas_decimais)
        parte_inteira = int(valor_arredondado)
        parte_decimal = abs(valor_arredondado - parte_inteira)

        # Format integer part
        parte_inteira_str = f"{parte_inteira:,}".replace(",", ".")

        # Format decimal part
        parte_decimal_str = f"{parte_decimal:.{casas_decimais}f}"[2:]  # Remove "0."

        return f"{parte_inteira_str},{parte_decimal_str}"


def formatar_percentual(valor: Union[float, int, None], casas_decimais: int = 0) -> str:
    """
    Format a numeric value as percentage.

    Args:
        valor: Numeric value (already as percentage, e.g., 75 for 75%)
        casas_decimais: Number of decimal places

    Returns:
        Formatted percentage string

    Examples:
        >>> formatar_percentual(75.5)
        "76%"
        >>> formatar_percentual(75.5, 1)
        "75,5%"
    """
    if valor is None:
        return "0%"

    if casas_decimais == 0:
        return f"{int(round(float(valor)))}%"
    else:
        valor_arredondado = round(float(valor), casas_decimais)
        parte_inteira = int(valor_arredondado)
        parte_decimal = abs(valor_arredondado - parte_inteira)
        parte_decimal_str = f"{parte_decimal:.{casas_decimais}f}"[2:]
        return f"{parte_inteira},{parte_decimal_str}%"
