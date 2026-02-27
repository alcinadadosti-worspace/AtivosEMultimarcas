"""
Export utilities for CSV and Excel files.
"""
import io
from typing import List, Dict, Any
import polars as pl


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
