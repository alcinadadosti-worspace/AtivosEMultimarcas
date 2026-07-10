"""
Reseller base (ConsultaRevendedores) — base permanente de cadastro.

Lê a planilha de revendedores (abas 13707/13706), normaliza a chave de
cruzamento e cruza com os pedidos importados (Mapa de Pedidos) para responder:
  - quantos revendedores compraram em cada ciclo;
  - quem está há mais ciclos sem comprar (CiclosInatividade da base);
  - em quais ciclos cada revendedor comprou (do arquivo multi-ciclo);
  - quem da base não comprou (lista de recuperação).
"""
import io
from typing import Any, Dict, List, Optional

import polars as pl

from app.config import (
    REV_SHEETS,
    REV_COL_CODIGO,
    REV_COL_NOME,
    REV_COL_SITUACAO,
    REV_COL_CICLOS_INATIVIDADE,
    REV_COL_PAPEL,
    REV_COL_COD_SETOR,
    REV_COL_SETOR,
    REV_COL_CICLO_PRIMEIRO,
    REV_COL_CICLO_CESSAMENTO,
    REV_COL_MOTIVO_CESSAMENTO,
    REV_COL_TELEFONE,
    REV_COL_CIDADE,
    REV_REQUIRED_COLUMNS,
    PED_COL_PESSOA,
    PED_COL_CICLO,
)

# Colunas mantidas da base (as demais são descartadas).
_KEEP = [
    REV_COL_CODIGO, REV_COL_NOME, REV_COL_SITUACAO, REV_COL_CICLOS_INATIVIDADE,
    REV_COL_PAPEL, REV_COL_COD_SETOR, REV_COL_SETOR, REV_COL_CICLO_PRIMEIRO,
    REV_COL_CICLO_CESSAMENTO, REV_COL_MOTIVO_CESSAMENTO, REV_COL_TELEFONE, REV_COL_CIDADE,
]


def _norm_cod(col: str) -> pl.Expr:
    """Só dígitos — casa '35.789' (base) com '35789' (Pessoa dos pedidos)."""
    return pl.col(col).cast(pl.Utf8).fill_null("").str.replace_all(r"[^0-9]", "")


def _sem_gb(col: str) -> pl.Expr:
    """Padroniza o Papel tirando o sufixo ' GB' (Diamante GB -> Diamante)."""
    return pl.col(col).cast(pl.Utf8).fill_null("").str.replace(r"(?i)\s*GB\s*$", "").str.strip_chars()


def processar_planilha_revendedores(content: bytes, filename: str) -> Dict[str, Any]:
    """Lê a base (todas as abas), normaliza e concatena com a coluna _unidade."""
    if filename.lower().endswith(".csv"):
        raise ValueError("A base de revendedores precisa ser um Excel com as abas 13707 e 13706.")

    try:
        planilhas = pl.read_excel(io.BytesIO(content), sheet_id=0, infer_schema_length=0)
    except Exception as e:
        raise ValueError(f"Não consegui ler as abas da planilha: {e}")

    if not isinstance(planilhas, dict):
        planilhas = {"base": planilhas}

    frames: List[pl.DataFrame] = []
    for nome_aba, df in planilhas.items():
        if REV_COL_CODIGO not in df.columns:
            continue
        # Preenche colunas opcionais ausentes.
        for c in _KEEP:
            if c not in df.columns:
                df = df.with_columns(pl.lit("").alias(c))
        df = df.select(_KEEP)
        unidade = REV_SHEETS.get(str(nome_aba).strip(), str(nome_aba).strip())
        df = df.with_columns([
            pl.lit(unidade).alias("_unidade"),
            pl.lit(str(nome_aba).strip()).alias("_cod_unidade"),
        ])
        frames.append(df)

    if not frames:
        raise ValueError(
            "Nenhuma aba com a coluna 'CodigoRevendedor' encontrada. "
            "Esta é a planilha de Consulta de Revendedores?"
        )

    base = pl.concat(frames, how="vertical_relaxed")

    # Normalizações.
    base = base.with_columns([
        _norm_cod(REV_COL_CODIGO).alias("_cod"),
        pl.col(REV_COL_NOME).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_nome"),
        pl.col(REV_COL_SITUACAO).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_situacao"),
        _sem_gb(REV_COL_PAPEL).alias("_segmento"),
        pl.col(REV_COL_SETOR).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_setor"),
        pl.col(REV_COL_COD_SETOR).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_setor_cod"),
        pl.col(REV_COL_CICLO_PRIMEIRO).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_ciclo_primeiro"),
        pl.col(REV_COL_MOTIVO_CESSAMENTO).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_motivo_cessamento"),
        pl.col(REV_COL_TELEFONE).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_telefone"),
        pl.col(REV_COL_CIDADE).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_cidade"),
        pl.col(REV_COL_CICLOS_INATIVIDADE)
          .cast(pl.Utf8).fill_null("0").str.replace_all(r"[^0-9-]", "")
          .str.replace("^$", "0").cast(pl.Int64, strict=False).fill_null(0).alias("_inatividade"),
    ])
    # Remove linhas sem código e deduplica por código (mantém a 1ª ocorrência).
    base = base.filter(pl.col("_cod") != "").unique(subset=["_cod"], keep="first")

    ativos = int(base.filter(pl.col("_situacao").str.to_lowercase() == "ativo").height)
    estatisticas = {
        "total": base.height,
        "ativos": ativos,
        "inativos": base.height - ativos,
        "por_unidade": {
            r["_unidade"]: int(r["n"])
            for r in base.group_by("_unidade").agg(pl.len().alias("n")).iter_rows(named=True)
        },
        "arquivo": filename,
    }
    return {"df": base, "estatisticas": estatisticas}


# ─────────────────────────────────────────────────────────────────────────────
# Cruzamento base × pedidos
# ─────────────────────────────────────────────────────────────────────────────

def _ped_compras(df_ped: pl.DataFrame) -> pl.DataFrame:
    """Por revendedor (código normalizado): ciclos comprados + totais."""
    d = df_ped.with_columns([
        _norm_cod(PED_COL_PESSOA).alias("_cod"),
        pl.col(PED_COL_CICLO).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_ciclo"),
    ]).filter(pl.col("_cod") != "")
    return (
        d.group_by("_cod").agg([
            pl.col("_ciclo").filter(pl.col("_ciclo") != "").n_unique().alias("qtd_ciclos"),
            pl.col("_ciclo").filter(pl.col("_ciclo") != "").unique().sort().alias("ciclos"),
            pl.col("_itens").sum().alias("itens"),
            pl.col("_valor").sum().alias("valor"),
            pl.len().alias("pedidos"),
        ])
    )


def _filtrar_unidade(df: pl.DataFrame, unidade: Optional[str]) -> pl.DataFrame:
    if unidade:
        return df.filter(pl.col("_cod_unidade") == str(unidade))
    return df


def ciclos_do_arquivo(df_ped: pl.DataFrame) -> List[str]:
    vals = (
        df_ped.select(pl.col(PED_COL_CICLO).cast(pl.Utf8).str.strip_chars())
        .to_series().drop_nulls().unique().to_list()
    )
    return sorted(c for c in vals if c)


def cobertura_resumo(df_rev: pl.DataFrame, df_ped: pl.DataFrame, unidade: Optional[str] = None) -> Dict[str, Any]:
    rev = _filtrar_unidade(df_rev, unidade)
    compras = _ped_compras(df_ped)
    comprou_set = set(compras.select("_cod").to_series().to_list())

    total = rev.height
    ativos = int(rev.filter(pl.col("_situacao").str.to_lowercase() == "ativo").height)
    rev = rev.with_columns(pl.col("_cod").is_in(list(comprou_set)).alias("_comprou"))
    compraram = int(rev.filter(pl.col("_comprou")).height)
    nunca = total - compraram
    ativos_nunca = int(rev.filter((~pl.col("_comprou")) & (pl.col("_situacao").str.to_lowercase() == "ativo")).height)
    ciclos = ciclos_do_arquivo(df_ped)
    return {
        "base_total": total,
        "ativos": ativos,
        "inativos": total - ativos,
        "compraram": compraram,
        "nunca_compraram": nunca,
        "ativos_nunca_compraram": ativos_nunca,
        "cobertura_pct": round(compraram / total * 100, 1) if total else 0.0,
        "ciclos": ciclos,
        "n_ciclos": len(ciclos),
    }


def cobertura_por_ciclo(df_ped: pl.DataFrame, unidade: Optional[str] = None) -> List[Dict[str, Any]]:
    d = df_ped
    if unidade and "_cod_unidade" in d.columns:
        d = d.filter(pl.col("_cod_unidade") == str(unidade))
    d = d.with_columns([
        _norm_cod(PED_COL_PESSOA).alias("_cod"),
        pl.col(PED_COL_CICLO).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_ciclo"),
    ]).filter((pl.col("_cod") != "") & (pl.col("_ciclo") != ""))
    res = (
        d.group_by("_ciclo").agg([
            pl.col("_cod").n_unique().alias("revendedores"),
            pl.col("_itens").sum().alias("itens"),
            pl.col("_valor").sum().alias("valor"),
            pl.len().alias("pedidos"),
        ]).sort("_ciclo")
    )
    return [
        {
            "ciclo": r["_ciclo"],
            "revendedores": int(r["revendedores"]),
            "itens": int(r["itens"]),
            "valor": round(float(r["valor"]), 2),
            "pedidos": int(r["pedidos"]),
        }
        for r in res.iter_rows(named=True)
    ]


def cobertura_frequencia(df_rev: pl.DataFrame, df_ped: pl.DataFrame, unidade: Optional[str] = None) -> List[Dict[str, Any]]:
    """Distribuição: quantos revendedores da base compraram em N ciclos (0..N)."""
    rev = _filtrar_unidade(df_rev, unidade).select("_cod")
    compras = _ped_compras(df_ped).select(["_cod", "qtd_ciclos"])
    joined = rev.join(compras, on="_cod", how="left").with_columns(
        pl.col("qtd_ciclos").fill_null(0)
    )
    dist = joined.group_by("qtd_ciclos").agg(pl.len().alias("revendedores")).sort("qtd_ciclos")
    return [
        {"qtd_ciclos": int(r["qtd_ciclos"]), "revendedores": int(r["revendedores"])}
        for r in dist.iter_rows(named=True)
    ]


def cobertura_revendedores(
    df_rev: pl.DataFrame,
    df_ped: pl.DataFrame,
    unidade: Optional[str] = None,
    filtro: str = "todos",           # todos | compraram | nunca | ativos_nunca
    ordenar: str = "inatividade",    # inatividade | qtd_ciclos | nome
    limite: int = 500,
) -> List[Dict[str, Any]]:
    """Lista de revendedores cruzada, para tabela/ranking e exportação."""
    rev = _filtrar_unidade(df_rev, unidade)
    compras = _ped_compras(df_ped)
    joined = rev.join(compras, on="_cod", how="left").with_columns([
        pl.col("qtd_ciclos").fill_null(0),
        pl.col("pedidos").fill_null(0),
        pl.col("itens").fill_null(0),
        pl.col("valor").fill_null(0.0),
    ])
    # "comprou" = presente nos pedidos (mesma definição do resumo), robusto a
    # ciclos em branco (que zerariam qtd_ciclos sem zerar a presença).
    joined = joined.with_columns((pl.col("pedidos") > 0).alias("_comprou"))

    if filtro == "compraram":
        joined = joined.filter(pl.col("_comprou"))
    elif filtro == "nunca":
        joined = joined.filter(~pl.col("_comprou"))
    elif filtro == "ativos_nunca":
        joined = joined.filter((~pl.col("_comprou")) & (pl.col("_situacao").str.to_lowercase() == "ativo"))

    if ordenar == "qtd_ciclos":
        joined = joined.sort(["qtd_ciclos", "_inatividade"], descending=[True, True])
    elif ordenar == "nome":
        joined = joined.sort("_nome")
    else:  # inatividade
        joined = joined.sort(["_inatividade", "qtd_ciclos"], descending=[True, False])

    joined = joined.head(limite)
    out = []
    for r in joined.iter_rows(named=True):
        out.append({
            "codigo": r["_cod"],
            "nome": r["_nome"] or "—",
            "situacao": r["_situacao"],
            "segmento": r["_segmento"],
            "unidade": r["_unidade"],
            "setor": r["_setor"],
            "cidade": r["_cidade"],
            "telefone": r["_telefone"],
            "inatividade": int(r["_inatividade"]),
            "qtd_ciclos": int(r["qtd_ciclos"]),
            "ciclos": list(r["ciclos"]) if r.get("ciclos") is not None else [],
            "itens": int(r["itens"]),
            "valor": round(float(r["valor"]), 2),
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Alerta de inatividade — clientes há X ciclos sem comprar (base)
# ─────────────────────────────────────────────────────────────────────────────

def _filtro_alerta(df_rev: pl.DataFrame, unidade, min_c: int, max_c: int) -> pl.DataFrame:
    d = _filtrar_unidade(df_rev, unidade)
    return d.filter((pl.col("_inatividade") >= min_c) & (pl.col("_inatividade") <= max_c))


def alerta_resumo(df_rev, unidade=None, min_c: int = 5, max_c: int = 7) -> Dict[str, Any]:
    d = _filtro_alerta(df_rev, unidade, min_c, max_c)
    return {
        "total": d.height,
        "por_ciclo": {
            int(c): int(d.filter(pl.col("_inatividade") == c).height)
            for c in range(min_c, max_c + 1)
        },
        "cidades": d.select("_cidade").filter(pl.col("_cidade") != "").n_unique(),
        "min": min_c, "max": max_c,
    }


def alerta_por_cidade(df_rev, unidade=None, min_c: int = 5, max_c: int = 7) -> List[Dict[str, Any]]:
    """Quantidade de clientes em alerta por cidade de cadastro (residencial)."""
    d = _filtro_alerta(df_rev, unidade, min_c, max_c)
    if d.is_empty():
        return []
    d = d.with_columns(
        pl.when(pl.col("_cidade") == "").then(pl.lit("Não informado")).otherwise(pl.col("_cidade")).alias("_cid")
    )
    g = (
        d.group_by("_cid").agg([
            pl.len().alias("total"),
            pl.col("_inatividade").max().alias("pior"),
        ]).sort("total", descending=True)
    )
    return [
        {"cidade": r["_cid"], "total": int(r["total"]), "pior": int(r["pior"])}
        for r in g.iter_rows(named=True)
    ]


def alerta_detalhe_cidade(df_rev, cidade: str, unidade=None, min_c: int = 5, max_c: int = 7) -> List[Dict[str, Any]]:
    """Lista de clientes em alerta de uma cidade (ordenados do pior ao menos)."""
    d = _filtro_alerta(df_rev, unidade, min_c, max_c)
    d = d.filter(pl.col("_cidade").str.to_lowercase() == cidade.strip().lower())
    d = d.sort("_inatividade", descending=True)
    return [
        {
            "codigo": r["_cod"],
            "nome": r["_nome"] or "—",
            "inatividade": int(r["_inatividade"]),
            "situacao": r["_situacao"],
            "segmento": r["_segmento"],
            "setor": r["_setor"],
            "unidade": r["_unidade"],
            "telefone": r["_telefone"],
        }
        for r in d.iter_rows(named=True)
    ]


def obter_unidades(df_rev: pl.DataFrame) -> List[Dict[str, str]]:
    u = df_rev.select(["_cod_unidade", "_unidade"]).unique().sort("_unidade")
    return [{"codigo": r["_cod_unidade"], "nome": r["_unidade"]} for r in u.iter_rows(named=True)]
