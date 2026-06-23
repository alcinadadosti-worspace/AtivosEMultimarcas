"""
Export utilities for CSV and Excel files.
"""
import io
from typing import List, Dict, Any, Optional
import polars as pl


# Campos usados para avaliar o status do setor (real x meta da planilha).
_CAMPOS_STATUS = [
    "receita",
    "clientes_ativos",
    "rpa",
    "percent_multimarcas",
    "percent_cabelos",
    "percent_make",
]


def _pior_pct(real: Dict[str, Any], meta: Dict[str, Any]) -> Optional[float]:
    """Menor % atingido entre os indicadores que têm meta cadastrada (> 0).

    Espelha a lógica de ``piorPct`` usada no front-end. Retorna None quando
    nenhum indicador tem meta definida.
    """
    pior, tem_meta = None, False
    for campo in _CAMPOS_STATUS:
        alvo = meta.get(campo) or 0
        if alvo and alvo > 0:
            tem_meta = True
            p = (real.get(campo) or 0) / alvo * 100
            if pior is None or p < pior:
                pior = p
    return pior if tem_meta else None


def _status_label(pct: Optional[float]) -> str:
    if pct is None:
        return "Sem meta"
    if pct >= 100:
        return "Meta batida"
    if pct >= 60:
        return "Quase lá"
    return "Precisa melhorar"


def _pct_da_meta(real: Any, meta: Any) -> Optional[float]:
    """Percentual atingido (real / meta * 100), ou None se não há meta."""
    if not meta or meta <= 0:
        return None
    return round((real or 0) / meta * 100, 1)


def exportar_metas_excel(metricas: List[Dict[str, Any]]) -> bytes:
    """Exporta as metas por setor para um Excel bem organizado.

    Apenas setores com meta cadastrada (``meta_planilha`` preenchida) são
    incluídos — setores/supervisoras sem meta ficam de fora, conforme pedido.

    Para cada indicador são geradas três colunas: valor realizado, meta e
    percentual atingido (% da meta), com formatação de moeda/percentual.
    """
    linhas: List[Dict[str, Any]] = []

    for m in metricas:
        meta = m.get("meta_planilha")
        if not meta:
            # Sem meta cadastrada → não entra na exportação.
            continue

        pct = _pior_pct(m, meta)
        linhas.append({
            "Setor":                       m.get("setor", ""),
            "Supervisora":                 m.get("supervisora", "") or "",
            "Status":                      _status_label(pct),
            "Receita Realizada":           round(float(m.get("receita") or 0), 2),
            "Meta de Receita":             round(float(meta.get("receita") or 0), 2),
            "Receita (% da Meta)":         _pct_da_meta(m.get("receita"), meta.get("receita")),
            "Clientes Ativos":             int(m.get("clientes_ativos") or 0),
            "Meta de Clientes Ativos":     int(meta.get("clientes_ativos") or 0),
            "Clientes Ativos (% da Meta)": _pct_da_meta(m.get("clientes_ativos"), meta.get("clientes_ativos")),
            "RPA Realizado":               round(float(m.get("rpa") or 0), 2),
            "Meta de RPA":                 round(float(meta.get("rpa") or 0), 2),
            "RPA (% da Meta)":             _pct_da_meta(m.get("rpa"), meta.get("rpa")),
            "Multimarca % Realizado":      round(float(m.get("percent_multimarcas") or 0), 1),
            "Meta Multimarca %":           round(float(meta.get("percent_multimarcas") or 0), 1),
            "Multimarca (% da Meta)":      _pct_da_meta(m.get("percent_multimarcas"), meta.get("percent_multimarcas")),
            "Cabelo % Realizado":          round(float(m.get("percent_cabelos") or 0), 1),
            "Meta Cabelo %":               round(float(meta.get("percent_cabelos") or 0), 1),
            "Cabelo (% da Meta)":          _pct_da_meta(m.get("percent_cabelos"), meta.get("percent_cabelos")),
            "Make % Realizado":            round(float(m.get("percent_make") or 0), 1),
            "Meta Make %":                 round(float(meta.get("percent_make") or 0), 1),
            "Make (% da Meta)":            _pct_da_meta(m.get("percent_make"), meta.get("percent_make")),
        })

    if not linhas:
        # DataFrame vazio (apenas cabeçalhos) — o chamador decide o que fazer.
        df = pl.DataFrame()
    else:
        df = pl.DataFrame(linhas)

    moeda = 'R$ #,##0.00'
    inteiro = '#,##0'
    pct_fmt = '0.0"%"'
    column_formats = {
        "Receita Realizada": moeda,
        "Meta de Receita": moeda,
        "Receita (% da Meta)": pct_fmt,
        "Clientes Ativos": inteiro,
        "Meta de Clientes Ativos": inteiro,
        "Clientes Ativos (% da Meta)": pct_fmt,
        "RPA Realizado": moeda,
        "Meta de RPA": moeda,
        "RPA (% da Meta)": pct_fmt,
        "Multimarca % Realizado": pct_fmt,
        "Meta Multimarca %": pct_fmt,
        "Multimarca (% da Meta)": pct_fmt,
        "Cabelo % Realizado": pct_fmt,
        "Meta Cabelo %": pct_fmt,
        "Cabelo (% da Meta)": pct_fmt,
        "Make % Realizado": pct_fmt,
        "Meta Make %": pct_fmt,
        "Make (% da Meta)": pct_fmt,
    }

    buffer = io.BytesIO()
    if df.is_empty():
        df.write_excel(buffer, worksheet="Metas")
    else:
        df.write_excel(
            buffer,
            worksheet="Metas",
            autofit=True,
            column_formats={k: v for k, v in column_formats.items() if k in df.columns},
            header_format={"bold": True, "bg_color": "#6D28D9", "font_color": "#FFFFFF", "border": 1},
        )
    buffer.seek(0)
    return buffer.getvalue()


def exportar_csv(df: pl.DataFrame) -> bytes:
    """
    Export a Polars DataFrame to CSV bytes.

    Args:
        df: Polars DataFrame to export

    Returns:
        CSV file as bytes
    """
    buffer = io.BytesIO()
    df.write_csv(buffer)
    buffer.seek(0)
    return buffer.getvalue()


def exportar_excel(df: pl.DataFrame, sheet_name: str = "Dados") -> bytes:
    """
    Export a Polars DataFrame to Excel bytes.

    Args:
        df: Polars DataFrame to export
        sheet_name: Name of the Excel sheet

    Returns:
        Excel file as bytes
    """
    buffer = io.BytesIO()
    df.write_excel(buffer, worksheet=sheet_name)
    buffer.seek(0)
    return buffer.getvalue()


def exportar_multiplas_abas(
    dataframes: Dict[str, pl.DataFrame]
) -> bytes:
    """
    Export multiple DataFrames to a single Excel file with multiple sheets.

    Args:
        dataframes: Dictionary mapping sheet names to DataFrames

    Returns:
        Excel file as bytes
    """
    buffer = io.BytesIO()

    # Use xlsxwriter for multiple sheets
    import xlsxwriter

    workbook = xlsxwriter.Workbook(buffer, {'in_memory': True})

    for sheet_name, df in dataframes.items():
        worksheet = workbook.add_worksheet(sheet_name[:31])  # Excel limit: 31 chars

        # Write headers
        for col_idx, col_name in enumerate(df.columns):
            worksheet.write(0, col_idx, col_name)

        # Write data
        for row_idx, row in enumerate(df.iter_rows(named=False)):
            for col_idx, value in enumerate(row):
                if value is None:
                    worksheet.write(row_idx + 1, col_idx, "")
                else:
                    worksheet.write(row_idx + 1, col_idx, value)

    workbook.close()
    buffer.seek(0)
    return buffer.getvalue()
