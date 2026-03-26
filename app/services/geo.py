"""
Geographic analysis service for client neighborhood and city data.
"""
from typing import Any, Dict, List, Optional

import polars as pl

from app.config import (
    GEO_COL_NOME,
    GEO_COL_CPF,
    GEO_COL_SITUACAO,
    GEO_COL_CICLOS_INATIVIDADE,
    GEO_COL_PAPEL,
    GEO_COL_COD_ESTRUTURA,
    GEO_COL_ESTRUTURA,
    GEO_COL_COD_ESTRUTURA_PAI,
    GEO_COL_TELEFONE,
    GEO_COL_RUA_RESID,
    GEO_COL_BAIRRO_RESID,
    GEO_COL_CIDADE_RESID,
    GEO_COL_BAIRRO_ENTREGA,
    GEO_COL_CIDADE_ENTREGA,
    GEO_REQUIRED_COLUMNS,
)
from app.services.venda import ler_planilha


# Optional columns filled with "" when absent
_OPTIONAL_COLS = [
    GEO_COL_CPF,
    GEO_COL_CICLOS_INATIVIDADE,
    GEO_COL_PAPEL,
    GEO_COL_COD_ESTRUTURA,
    GEO_COL_ESTRUTURA,
    GEO_COL_COD_ESTRUTURA_PAI,
    GEO_COL_TELEFONE,
    GEO_COL_RUA_RESID,
    GEO_COL_BAIRRO_ENTREGA,
    GEO_COL_CIDADE_ENTREGA,
]


def processar_planilha_clientes(content: bytes, filename: str) -> Dict[str, Any]:
    """
    Read and normalize the clients spreadsheet for geographic analysis.

    Returns:
        {df: pl.DataFrame, estatisticas: dict, avisos: list[str]}
    """
    avisos: List[str] = []

    df = ler_planilha(content, filename)

    # Validate required columns
    missing = [c for c in GEO_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {', '.join(missing)}")

    # Add missing optional columns as empty strings
    for col in _OPTIONAL_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit("").alias(col))
            avisos.append(f"Coluna '{col}' não encontrada — preenchida com vazio")

    # Keep only the columns we need
    all_cols = GEO_REQUIRED_COLUMNS + [c for c in _OPTIONAL_COLS if c in df.columns]
    df = df.select(all_cols)

    # Cast everything to string and fill nulls
    text_cols = [c for c in df.columns if c != GEO_COL_CICLOS_INATIVIDADE]
    df = df.with_columns(
        [pl.col(c).cast(pl.Utf8).fill_null("").str.strip_chars() for c in text_cols]
    )

    # Parse ciclos_inatividade as integer
    df = df.with_columns(
        pl.col(GEO_COL_CICLOS_INATIVIDADE)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace_all(r"[^\d]", "")
        .str.replace("^$", "0")
        .cast(pl.Int32, strict=False)
        .fill_null(0)
        .alias(GEO_COL_CICLOS_INATIVIDADE)
    )

    # Normalize situacao to uppercase for consistent comparison
    df = df.with_columns(
        pl.col(GEO_COL_SITUACAO).str.to_uppercase().alias(GEO_COL_SITUACAO)
    )

    # Normalize CodigoEstruturaComercialPai: '1.048' → '1048', '1.515' → '1515'
    if GEO_COL_COD_ESTRUTURA_PAI in df.columns:
        df = df.with_columns(
            pl.col(GEO_COL_COD_ESTRUTURA_PAI)
            .str.replace_all(r"\.", "")
            .alias(GEO_COL_COD_ESTRUTURA_PAI)
        )

    # Flag clients whose delivery address differs from residential address
    df = df.with_columns(
        (
            (pl.col(GEO_COL_BAIRRO_ENTREGA) != "")
            & (
                (
                    pl.col(GEO_COL_BAIRRO_RESID).str.to_lowercase()
                    != pl.col(GEO_COL_BAIRRO_ENTREGA).str.to_lowercase()
                )
                | (
                    pl.col(GEO_COL_CIDADE_RESID).str.to_lowercase()
                    != pl.col(GEO_COL_CIDADE_ENTREGA).str.to_lowercase()
                )
            )
        ).alias("endereco_diferente")
    )

    # Drop rows without a neighborhood
    df = df.filter(pl.col(GEO_COL_BAIRRO_RESID) != "")

    total = len(df)
    ativos = int(df.filter(pl.col(GEO_COL_SITUACAO).str.starts_with("ATIVO")).height)

    estatisticas = {
        "total": total,
        "ativos": ativos,
        "inativos": total - ativos,
        "com_endereco_diferente": int(df.filter(pl.col("endereco_diferente")).height),
        "cidades": df.select(GEO_COL_CIDADE_RESID).unique().height,
        "bairros": df.select(GEO_COL_BAIRRO_RESID).unique().height,
    }

    return {"df": df, "estatisticas": estatisticas, "avisos": avisos}


def calcular_metricas_bairro(
    df: pl.DataFrame,
    unidade: Optional[str] = None,
    cidade: Optional[str] = None,
    situacao: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Aggregate client metrics grouped by neighborhood.

    Each bairro item includes a pre-computed 'ruas' list so the frontend can
    show street-level detail instantly without a second API call.
    """
    df = _aplicar_filtros(df, unidade=unidade, cidade=cidade, situacao=situacao)

    # ── Bairro-level aggregation ──────────────────────────────────────────────
    result = (
        df.group_by([GEO_COL_BAIRRO_RESID, GEO_COL_CIDADE_RESID])
        .agg(
            [
                pl.len().alias("total"),
                pl.col(GEO_COL_SITUACAO).str.starts_with("ATIVO").sum().alias("ativos"),
                pl.col(GEO_COL_CICLOS_INATIVIDADE)
                .filter(~pl.col(GEO_COL_SITUACAO).str.starts_with("ATIVO"))
                .mean()
                .alias("media_ciclos_inativos"),
            ]
        )
        .with_columns((pl.col("total") - pl.col("ativos")).alias("inativos"))
        .sort("total", descending=True)
    )

    return [
        {
            "bairro": row[GEO_COL_BAIRRO_RESID] or "Não informado",
            "cidade": row[GEO_COL_CIDADE_RESID] or "",
            "total": int(row["total"]),
            "ativos": int(row["ativos"]),
            "inativos": int(row["inativos"]),
            "media_ciclos_inativos": round(row["media_ciclos_inativos"] or 0.0, 1),
        }
        for row in result.iter_rows(named=True)
    ]


def calcular_detalhe_bairro(
    df: pl.DataFrame,
    bairro: str,
    unidade: Optional[str] = None,
    situacao: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Return streets and individual clients for a specific neighborhood.

    Used to populate the accordion expansion in the bairros table.
    """
    df = _aplicar_filtros(df, unidade=unidade, situacao=situacao)
    df = df.filter(
        pl.col(GEO_COL_BAIRRO_RESID).str.to_lowercase() == bairro.strip().lower()
    )

    # Streets grouped summary
    ruas: List[Dict[str, Any]] = []
    if GEO_COL_RUA_RESID in df.columns:
        ruas_df = (
            df.group_by(GEO_COL_RUA_RESID)
            .agg(
                [
                    pl.len().alias("total"),
                    pl.col(GEO_COL_SITUACAO)
                    .str.starts_with("ATIVO")
                    .sum()
                    .alias("ativos"),
                ]
            )
            .with_columns((pl.col("total") - pl.col("ativos")).alias("inativos"))
            .sort("total", descending=True)
        )
        ruas = [
            {
                "rua": row[GEO_COL_RUA_RESID] or "Não informado",
                "total": int(row["total"]),
                "ativos": int(row["ativos"]),
                "inativos": int(row["inativos"]),
            }
            for row in ruas_df.iter_rows(named=True)
        ]

    # Individual clients sorted by inactivity cycles desc (capped at 150)
    df_sorted = df.sort(GEO_COL_CICLOS_INATIVIDADE, descending=True).head(150)
    clientes = [
        {
            "nome": row.get(GEO_COL_NOME, ""),
            "cpf": row.get(GEO_COL_CPF, ""),
            "rua": row.get(GEO_COL_RUA_RESID, ""),
            "situacao": row.get(GEO_COL_SITUACAO, ""),
            "ativo": str(row.get(GEO_COL_SITUACAO, "")).startswith("ATIVO"),
            "ciclos_inatividade": int(row.get(GEO_COL_CICLOS_INATIVIDADE, 0) or 0),
            "papel": row.get(GEO_COL_PAPEL, ""),
            "telefone": row.get(GEO_COL_TELEFONE, ""),
            "endereco_diferente": bool(row.get("endereco_diferente", False)),
            "bairro_entrega": row.get(GEO_COL_BAIRRO_ENTREGA, ""),
            "cidade_entrega": row.get(GEO_COL_CIDADE_ENTREGA, ""),
        }
        for row in df_sorted.iter_rows(named=True)
    ]

    return {"ruas": ruas, "clientes": clientes}


def calcular_metricas_cidade(
    df: pl.DataFrame,
    unidade: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Aggregate client metrics grouped by city (used for the heat map)."""
    df = _aplicar_filtros(df, unidade=unidade)

    result = (
        df.group_by(GEO_COL_CIDADE_RESID)
        .agg(
            [
                pl.len().alias("total"),
                pl.col(GEO_COL_SITUACAO).str.starts_with("ATIVO").sum().alias("ativos"),
            ]
        )
        .with_columns((pl.col("total") - pl.col("ativos")).alias("inativos"))
        .sort("total", descending=True)
    )

    return [
        {
            "cidade": row[GEO_COL_CIDADE_RESID] or "Não informado",
            "total": int(row["total"]),
            "ativos": int(row["ativos"]),
            "inativos": int(row["inativos"]),
        }
        for row in result.iter_rows(named=True)
    ]


def listar_clientes_geo(
    df: pl.DataFrame,
    unidade: Optional[str] = None,
    cidade: Optional[str] = None,
    bairro: Optional[str] = None,
    situacao: Optional[str] = None,
    ordenar_por: str = "ciclos_desc",
    limite: int = 300,
) -> List[Dict[str, Any]]:
    """List individual clients with their geographic and status data."""
    df = _aplicar_filtros(df, unidade=unidade, cidade=cidade, situacao=situacao)

    if bairro:
        df = df.filter(
            pl.col(GEO_COL_BAIRRO_RESID).str.to_lowercase() == bairro.strip().lower()
        )

    if ordenar_por == "ciclos_desc":
        df = df.sort(GEO_COL_CICLOS_INATIVIDADE, descending=True)
    elif ordenar_por == "ciclos_asc":
        df = df.sort(GEO_COL_CICLOS_INATIVIDADE, descending=False)
    elif ordenar_por == "nome":
        df = df.sort(GEO_COL_NOME)

    df = df.head(limite)

    return [
        {
            "nome": row.get(GEO_COL_NOME, ""),
            "cpf": row.get(GEO_COL_CPF, ""),
            "rua": row.get(GEO_COL_RUA_RESID, ""),
            "situacao": row.get(GEO_COL_SITUACAO, ""),
            "ativo": str(row.get(GEO_COL_SITUACAO, "")).startswith("ATIVO"),
            "ciclos_inatividade": int(row.get(GEO_COL_CICLOS_INATIVIDADE, 0) or 0),
            "papel": row.get(GEO_COL_PAPEL, ""),
            "estrutura": row.get(GEO_COL_ESTRUTURA, ""),
            "unidade": row.get(GEO_COL_COD_ESTRUTURA_PAI, ""),
            "telefone": row.get(GEO_COL_TELEFONE, ""),
            "bairro": row.get(GEO_COL_BAIRRO_RESID, ""),
            "cidade": row.get(GEO_COL_CIDADE_RESID, ""),
            "bairro_entrega": row.get(GEO_COL_BAIRRO_ENTREGA, ""),
            "cidade_entrega": row.get(GEO_COL_CIDADE_ENTREGA, ""),
            "endereco_diferente": bool(row.get("endereco_diferente", False)),
        }
        for row in df.iter_rows(named=True)
    ]


def obter_cidades_geo(df: pl.DataFrame) -> List[str]:
    """Return sorted unique city names from residential address."""
    return (
        df.select(GEO_COL_CIDADE_RESID)
        .filter(pl.col(GEO_COL_CIDADE_RESID) != "")
        .unique()
        .sort(GEO_COL_CIDADE_RESID)
        .to_series()
        .to_list()
    )


def obter_bairros_geo(df: pl.DataFrame, cidade: Optional[str] = None) -> List[str]:
    """Return sorted unique neighborhood names, optionally filtered by city."""
    filtered = df
    if cidade:
        filtered = df.filter(
            pl.col(GEO_COL_CIDADE_RESID).str.to_lowercase() == cidade.strip().lower()
        )
    return (
        filtered.select(GEO_COL_BAIRRO_RESID)
        .filter(pl.col(GEO_COL_BAIRRO_RESID) != "")
        .unique()
        .sort(GEO_COL_BAIRRO_RESID)
        .to_series()
        .to_list()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _aplicar_filtros(
    df: pl.DataFrame,
    unidade: Optional[str] = None,
    cidade: Optional[str] = None,
    situacao: Optional[str] = None,
) -> pl.DataFrame:
    if unidade and GEO_COL_COD_ESTRUTURA_PAI in df.columns:
        df = df.filter(pl.col(GEO_COL_COD_ESTRUTURA_PAI) == str(unidade))
    if cidade:
        df = df.filter(
            pl.col(GEO_COL_CIDADE_RESID).str.to_lowercase() == cidade.strip().lower()
        )
    if situacao:
        if situacao.upper() == "ATIVO":
            df = df.filter(pl.col(GEO_COL_SITUACAO).str.starts_with("ATIVO"))
        elif situacao.upper() == "INATIVO":
            df = df.filter(~pl.col(GEO_COL_SITUACAO).str.starts_with("ATIVO"))
    return df
