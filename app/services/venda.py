"""
Sales spreadsheet processing service.

This module handles reading, validating, and enriching sales data
from uploaded spreadsheets.
"""
import io
import sqlite3
from typing import Any, Dict, List, Optional, Callable

import polars as pl

from app.config import (
    VENDAS_COL_SETOR,
    VENDAS_COL_NOME_REVENDEDORA,
    VENDAS_COL_CODIGO_REVENDEDORA,
    VENDAS_COL_CICLO,
    VENDAS_COL_CODIGO_PRODUTO,
    VENDAS_COL_NOME_PRODUTO,
    VENDAS_COL_TIPO,
    VENDAS_COL_QTD_ITENS,
    VENDAS_COL_VALOR,
    VENDAS_COL_GERENCIA,
    VENDAS_REQUIRED_COLUMNS,
    TIPO_VENDA,
    MARCA_DESCONHECIDA,
    MOTIVO_NAO_ENCONTRADO,
    MOTIVO_MATCH_EXATO,
    MOTIVO_MATCH_COM_ZERO,
    MOTIVO_MATCH_SEM_ZERO,
)
from app.utils.normalizers import normalizar_sku
from app.services.produto import criar_indice_sku_em_memoria, buscar_sku_no_indice


def validar_colunas(df: pl.DataFrame) -> List[str]:
    """
    Validate that required columns exist in the DataFrame.

    Args:
        df: DataFrame to validate

    Returns:
        List of missing column names (empty if all present)
    """
    return [col for col in VENDAS_REQUIRED_COLUMNS if col not in df.columns]


def ler_planilha(file_bytes: bytes, filename: str) -> pl.DataFrame:
    """
    Read a spreadsheet file (CSV or Excel) into a DataFrame.

    Args:
        file_bytes: File content as bytes
        filename: Original filename (for format detection)

    Returns:
        Polars DataFrame

    Raises:
        ValueError: If file cannot be read
    """
    if filename.lower().endswith(('.xlsx', '.xls')):
        df = pl.read_excel(io.BytesIO(file_bytes), infer_schema_length=0)
    else:
        # Try multiple encodings for CSV
        df = None
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']

        for encoding in encodings:
            try:
                df = pl.read_csv(
                    io.BytesIO(file_bytes),
                    encoding=encoding,
                    infer_schema_length=0,
                    ignore_errors=True
                )
                break
            except Exception:
                continue

        if df is None:
            raise ValueError("Nao foi possivel ler o arquivo CSV com nenhum encoding")

    # Normalize column names (remove whitespace)
    df = df.rename({col: col.strip() for col in df.columns})

    return df


def processar_planilha_vendas(
    file_bytes: bytes,
    filename: str,
    conn: sqlite3.Connection,
    progress_callback: Optional[Callable[[float], None]] = None
) -> Dict[str, Any]:
    """
    Process a sales spreadsheet uploaded by the user.

    Steps:
    1. Detect format (CSV/XLSX) and encoding
    2. Validate required columns
    3. Normalize SKUs
    4. Cross-reference with product DB (find brand)
    5. Calculate match statistics

    Args:
        file_bytes: File content as bytes
        filename: Original filename (for format detection)
        conn: SQLite connection
        progress_callback: Function to report progress (0.0 to 1.0)

    Returns:
        Dict containing:
        - df: Complete enriched DataFrame
        - df_vendas: DataFrame filtered to sales only
        - estatisticas: Dict with processing metrics
        - avisos: List of warnings/alerts

    Raises:
        ValueError: If required columns are missing
    """
    avisos = []

    # 1. READ FILE
    df = ler_planilha(file_bytes, filename)

    # 2. VALIDATE REQUIRED COLUMNS
    colunas_faltando = validar_colunas(df)
    if colunas_faltando:
        raise ValueError(f"Colunas obrigatorias faltando: {', '.join(colunas_faltando)}")

    # 3. NORMALIZE SKUs
    df = df.with_columns([
        pl.col(VENDAS_COL_CODIGO_PRODUTO)
          .map_elements(normalizar_sku, return_dtype=pl.Utf8)
          .alias("CodigoProduto_normalizado")
    ])

    # 4. CREATE INDEX AND SEARCH BRANDS
    indice_sku = criar_indice_sku_em_memoria(conn)

    total_linhas = len(df)
    resultados_marca = []
    resultados_nome = []
    resultados_motivo = []

    for i, row in enumerate(df.iter_rows(named=True)):
        codigo = row["CodigoProduto_normalizado"]
        marca, nome, motivo = buscar_sku_no_indice(codigo, indice_sku)

        resultados_marca.append(marca if marca else MARCA_DESCONHECIDA)
        resultados_nome.append(nome if nome else row[VENDAS_COL_NOME_PRODUTO])
        resultados_motivo.append(motivo)

        # Report progress
        if progress_callback and i % 1000 == 0:
            progress_callback(i / total_linhas)

    # Add result columns
    df = df.with_columns([
        pl.Series("Marca_BD", resultados_marca),
        pl.Series("Nome_BD", resultados_nome),
        pl.Series("Motivo_Match", resultados_motivo),
    ])

    # 5. GENERATE CLIENT ID
    df = df.with_columns([
        pl.when(
            pl.col(VENDAS_COL_CODIGO_REVENDEDORA).is_not_null() &
            (pl.col(VENDAS_COL_CODIGO_REVENDEDORA).cast(pl.Utf8).str.strip_chars() != "")
        )
        .then(pl.col(VENDAS_COL_CODIGO_REVENDEDORA).cast(pl.Utf8).str.strip_chars())
        .otherwise(
            pl.concat_str([
                pl.col(VENDAS_COL_NOME_REVENDEDORA).cast(pl.Utf8),
                pl.lit("_"),
                pl.col(VENDAS_COL_SETOR).cast(pl.Utf8)
            ])
        )
        .alias("ClienteID")
    ])

    # 6. CALCULATE STATISTICS
    df_vendas = df.filter(pl.col(VENDAS_COL_TIPO).cast(pl.Utf8) == TIPO_VENDA)

    total_vendas = len(df_vendas)
    nao_encontrados = len(df_vendas.filter(pl.col("Motivo_Match") == MOTIVO_NAO_ENCONTRADO))
    match_exato = len(df_vendas.filter(pl.col("Motivo_Match") == MOTIVO_MATCH_EXATO))
    match_com_zero = len(df_vendas.filter(pl.col("Motivo_Match") == MOTIVO_MATCH_COM_ZERO))
    match_sem_zero = len(df_vendas.filter(pl.col("Motivo_Match") == MOTIVO_MATCH_SEM_ZERO))

    taxa_match = (total_vendas - nao_encontrados) / total_vendas if total_vendas > 0 else 0

    # Warnings
    avisos.append(f"Total de linhas processadas: {total_linhas}")
    avisos.append(f"Registros de venda: {total_vendas}")
    avisos.append(f"SKUs encontrados: {total_vendas - nao_encontrados} ({taxa_match*100:.1f}%)")

    if match_com_zero > 0:
        avisos.append(f"{match_com_zero} SKUs encontrados com match por zero a esquerda")

    if match_sem_zero > 0:
        avisos.append(f"{match_sem_zero} SKUs encontrados com match sem zero a esquerda")

    # Alert if many not found (> 5%)
    if total_vendas > 0 and nao_encontrados / total_vendas > 0.05:
        avisos.append(f"ALERTA: {nao_encontrados} SKUs ({nao_encontrados/total_vendas*100:.1f}%) nao encontrados no BD")

    if progress_callback:
        progress_callback(1.0)

    return {
        "df": df,
        "df_vendas": df_vendas,
        "estatisticas": {
            "total_linhas": total_linhas,
            "total_vendas": total_vendas,
            "encontrados": total_vendas - nao_encontrados,
            "nao_encontrados": nao_encontrados,
            "match_exato": match_exato,
            "match_com_zero": match_com_zero,
            "match_sem_zero": match_sem_zero,
            "taxa_match": taxa_match,
        },
        "avisos": avisos
    }


def obter_ciclos_unicos(df: pl.DataFrame) -> List[str]:
    """
    Get unique cycles from the DataFrame.

    Args:
        df: DataFrame with sales data

    Returns:
        Sorted list of unique cycle values
    """
    return sorted(
        df.select(pl.col(VENDAS_COL_CICLO).unique())
        .to_series()
        .to_list()
    )


def obter_setores_unicos(df: pl.DataFrame) -> List[str]:
    """
    Get unique sectors from the DataFrame.

    Args:
        df: DataFrame with sales data

    Returns:
        Sorted list of unique sector values
    """
    return sorted(
        df.select(pl.col(VENDAS_COL_SETOR).cast(pl.Utf8).unique())
        .to_series()
        .to_list()
    )


def obter_marcas_unicas(df: pl.DataFrame) -> List[str]:
    """
    Get unique brands from the DataFrame.

    Args:
        df: DataFrame with enriched sales data

    Returns:
        Sorted list of unique brand values
    """
    if "Marca_BD" not in df.columns:
        return []

    return sorted(
        df.select(pl.col("Marca_BD").unique())
        .to_series()
        .to_list()
    )


def obter_gerencias_unicas(df: pl.DataFrame) -> List[str]:
    """
    Get unique management codes from the DataFrame.

    Args:
        df: DataFrame with sales data

    Returns:
        Sorted list of unique management codes
    """
    if VENDAS_COL_GERENCIA not in df.columns:
        return []

    return sorted(
        df.select(pl.col(VENDAS_COL_GERENCIA).cast(pl.Utf8).unique())
        .filter(pl.col(VENDAS_COL_GERENCIA).is_not_null())
        .to_series()
        .to_list()
    )
