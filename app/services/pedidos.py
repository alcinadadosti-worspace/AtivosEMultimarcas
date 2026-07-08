"""
Orders spreadsheet processing service — "Mapa de Pedidos".

Reads the ConsultaPedidos export and builds a realistic geographic view of
resellers/orders across Alagoas municipalities.

Key rule ("cidade de moradia"):
    - Delivery at home  ("No endereço de entrega")     -> CidadeEntregaRetirada
    - Pickup at the unit ("Retirar na central...")     -> Cidade (cadastro)

  Because on pickup orders the CidadeEntregaRetirada is the unit's city
  (Penedo / Palmeira dos Índios), not where the reseller actually lives.
"""
from typing import Any, Dict, List, Optional

import polars as pl

from app.config import (
    PED_COL_PESSOA,
    PED_COL_NOME,
    PED_COL_PAPEL,
    PED_COL_QTDE_MATERIAIS,
    PED_COL_VALOR,
    PED_COL_TIPO_ENTREGA,
    PED_COL_CICLO,
    PED_COL_ESTRUTURA_PAI,
    PED_COL_COD_ESTRUTURA,
    PED_COL_ESTRUTURA,
    PED_COL_TELEFONE,
    PED_COL_LOGRADOURO,
    PED_COL_BAIRRO,
    PED_COL_CIDADE,
    PED_COL_LOGRADOURO_ENTREGA,
    PED_COL_BAIRRO_ENTREGA,
    PED_COL_CIDADE_ENTREGA,
    PED_TIPO_RETIRADA,
    PED_TIPO_ENTREGA_CASA,
    PED_REQUIRED_COLUMNS,
    PED_UNIDADES,
)
from app.services.venda import ler_planilha

# Ordem canônica das segmentações (metais + demais papéis), do topo para a base.
SEGMENTOS_ORDEM = [
    "Diamante GB",
    "Esmeralda GB",
    "Rubi",
    "Platina",
    "Ouro",
    "Prata",
    "Bronze",
    "Cobre",
    "Revendedor",
    "Consumidor Final",
]

# Colunas opcionais preenchidas com "" quando ausentes.
_OPTIONAL_COLS = [
    PED_COL_NOME,
    PED_COL_CICLO,
    PED_COL_ESTRUTURA_PAI,
    PED_COL_COD_ESTRUTURA,
    PED_COL_ESTRUTURA,
    PED_COL_TELEFONE,
    PED_COL_LOGRADOURO,
    PED_COL_BAIRRO,
    PED_COL_CIDADE,
    PED_COL_LOGRADOURO_ENTREGA,
    PED_COL_BAIRRO_ENTREGA,
]


def _num_float(col: str) -> pl.Expr:
    """Parse a value column into Float64 (handles '1.659,60' and '1659.6')."""
    limpo = (
        pl.col(col).cast(pl.Utf8).fill_null("").str.replace_all(r"[^\d,.-]", "")
    )
    # Com vírgula = formato BR: ponto é milhar (remove) e vírgula é decimal.
    br = limpo.str.replace_all(r"\.", "").str.replace(",", ".")
    return (
        pl.when(limpo.str.contains(","))
        .then(br)
        .otherwise(limpo)
        .cast(pl.Float64, strict=False)
        .fill_null(0.0)
    )


def _num_int(col: str) -> pl.Expr:
    """Parse a quantity column into Int64."""
    return (
        pl.col(col)
        .cast(pl.Utf8)
        .fill_null("")
        .str.replace_all(r"[^\d-]", "")
        .str.replace("^$", "0")
        .cast(pl.Int64, strict=False)
        .fill_null(0)
    )


def processar_planilha_pedidos(content: bytes, filename: str) -> Dict[str, Any]:
    """
    Read and normalize the ConsultaPedidos spreadsheet.

    Returns:
        {df, estatisticas, avisos, ciclo}
    """
    avisos: List[str] = []
    df = ler_planilha(content, filename)

    missing = [c for c in PED_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            "Esta não parece ser a planilha de Consulta de Pedidos. "
            f"Colunas obrigatórias ausentes: {', '.join(missing)}"
        )

    for col in _OPTIONAL_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit("").alias(col))
            avisos.append(f"Coluna '{col}' não encontrada — preenchida com vazio.")

    # Texto: limpa e normaliza espaços em branco.
    text_cols = [
        PED_COL_PESSOA, PED_COL_NOME, PED_COL_PAPEL, PED_COL_TIPO_ENTREGA,
        PED_COL_CICLO, PED_COL_ESTRUTURA_PAI, PED_COL_COD_ESTRUTURA,
        PED_COL_ESTRUTURA, PED_COL_TELEFONE, PED_COL_LOGRADOURO, PED_COL_BAIRRO,
        PED_COL_CIDADE, PED_COL_LOGRADOURO_ENTREGA, PED_COL_BAIRRO_ENTREGA,
        PED_COL_CIDADE_ENTREGA,
    ]
    df = df.with_columns(
        [pl.col(c).cast(pl.Utf8).fill_null("").str.strip_chars() for c in text_cols]
    )

    # Numéricos.
    df = df.with_columns([
        _num_int(PED_COL_QTDE_MATERIAIS).alias("_itens"),
        _num_float(PED_COL_VALOR).alias("_valor"),
    ])

    # Unidade gerenciadora a partir do prefixo numérico de EstruturaPai.
    df = df.with_columns(
        pl.col(PED_COL_ESTRUTURA_PAI)
        .str.extract(r"(\d+)", 1)
        .fill_null("")
        .alias("_cod_unidade")
    )
    df = df.with_columns(
        pl.col("_cod_unidade")
        .replace_strict(PED_UNIDADES, default="Outra/Sem unidade")
        .alias("_unidade")
    )

    # Tipo de visita legível.
    df = df.with_columns(
        pl.when(pl.col(PED_COL_TIPO_ENTREGA) == PED_TIPO_RETIRADA)
        .then(pl.lit("Retirou na unidade"))
        .when(pl.col(PED_COL_TIPO_ENTREGA) == PED_TIPO_ENTREGA_CASA)
        .then(pl.lit("Recebeu em casa"))
        .otherwise(pl.lit("Outro"))
        .alias("_tipo_visita")
    )
    veio_unidade = pl.col(PED_COL_TIPO_ENTREGA) == PED_TIPO_RETIRADA

    # Cidade/bairro/logradouro de MORADIA (regra realista).
    def _moradia(cadastro: str, entrega: str) -> pl.Expr:
        # Retirada na unidade -> cadastro; entrega em casa -> endereço de entrega.
        base = (
            pl.when(veio_unidade)
            .then(pl.col(cadastro))
            .otherwise(
                pl.when(pl.col(entrega) != "").then(pl.col(entrega)).otherwise(pl.col(cadastro))
            )
        )
        return base.str.strip_chars()

    df = df.with_columns([
        _moradia(PED_COL_CIDADE, PED_COL_CIDADE_ENTREGA).alias("_cidade_moradia"),
        _moradia(PED_COL_BAIRRO, PED_COL_BAIRRO_ENTREGA).alias("_bairro_moradia"),
        _moradia(PED_COL_LOGRADOURO, PED_COL_LOGRADOURO_ENTREGA).alias("_rua_moradia"),
    ])
    df = df.with_columns(
        pl.when(pl.col("_cidade_moradia") == "")
        .then(pl.lit("Não informado"))
        .otherwise(pl.col("_cidade_moradia"))
        .alias("_cidade_moradia")
    )

    # Segmento (Papel), com fallback.
    df = df.with_columns(
        pl.when(pl.col(PED_COL_PAPEL) == "")
        .then(pl.lit("Sem papel"))
        .otherwise(pl.col(PED_COL_PAPEL))
        .alias("_segmento")
    )

    # Ciclo legível: "10/2026" -> "10".
    ciclo_serie = (
        df.select(pl.col(PED_COL_CICLO).str.extract(r"^(\d+)", 1))
        .to_series()
        .drop_nulls()
    )
    ciclo = ciclo_serie[0] if len(ciclo_serie) else ""

    estatisticas = _resumo(df)
    estatisticas["ciclo"] = ciclo
    estatisticas["arquivo"] = filename

    return {"df": df, "estatisticas": estatisticas, "avisos": avisos, "ciclo": ciclo}


# ─────────────────────────────────────────────────────────────────────────────
# Agregações
# ─────────────────────────────────────────────────────────────────────────────

def _aplicar_filtros(
    df: pl.DataFrame,
    unidade: Optional[str] = None,
    segmento: Optional[str] = None,
    tipo_visita: Optional[str] = None,
    cidade: Optional[str] = None,
) -> pl.DataFrame:
    if unidade:
        df = df.filter(pl.col("_cod_unidade") == str(unidade))
    if segmento:
        df = df.filter(pl.col("_segmento") == segmento)
    if tipo_visita == "unidade":
        df = df.filter(pl.col("_tipo_visita") == "Retirou na unidade")
    elif tipo_visita == "casa":
        df = df.filter(pl.col("_tipo_visita") == "Recebeu em casa")
    if cidade:
        df = df.filter(
            pl.col("_cidade_moradia").str.to_lowercase() == cidade.strip().lower()
        )
    return df


def _resumo(df: pl.DataFrame) -> Dict[str, Any]:
    if df.is_empty():
        return {
            "pedidos": 0, "revendedores": 0, "itens": 0, "faturamento": 0.0,
            "cidades": 0, "visitaram_unidade": 0, "receberam_casa": 0,
            "ticket_medio": 0.0,
        }
    pedidos = df.height
    revendedores = df.select(PED_COL_PESSOA).filter(pl.col(PED_COL_PESSOA) != "").n_unique()
    itens = int(df.select(pl.col("_itens").sum()).item() or 0)
    faturamento = float(df.select(pl.col("_valor").sum()).item() or 0.0)
    return {
        "pedidos": pedidos,
        "revendedores": int(revendedores),
        "itens": itens,
        "faturamento": round(faturamento, 2),
        "cidades": df.select("_cidade_moradia").filter(pl.col("_cidade_moradia") != "Não informado").n_unique(),
        "visitaram_unidade": int(df.filter(pl.col("_tipo_visita") == "Retirou na unidade").height),
        "receberam_casa": int(df.filter(pl.col("_tipo_visita") == "Recebeu em casa").height),
        "ticket_medio": round(faturamento / pedidos, 2) if pedidos else 0.0,
    }


def calcular_resumo(df: pl.DataFrame, **filtros) -> Dict[str, Any]:
    return _resumo(_aplicar_filtros(df, **filtros))


def calcular_por_cidade(df: pl.DataFrame, **filtros) -> List[Dict[str, Any]]:
    """Métricas agregadas por cidade de moradia (base do mapa e do ranking)."""
    df = _aplicar_filtros(df, **filtros)
    if df.is_empty():
        return []
    result = (
        df.group_by("_cidade_moradia")
        .agg([
            pl.col(PED_COL_PESSOA).filter(pl.col(PED_COL_PESSOA) != "").n_unique().alias("revendedores"),
            pl.len().alias("pedidos"),
            pl.col("_itens").sum().alias("itens"),
            pl.col("_valor").sum().alias("valor"),
            (pl.col("_tipo_visita") == "Retirou na unidade").sum().alias("visitaram_unidade"),
            (pl.col("_tipo_visita") == "Recebeu em casa").sum().alias("receberam_casa"),
        ])
        .sort("revendedores", descending=True)
    )
    return [
        {
            "cidade": row["_cidade_moradia"],
            "revendedores": int(row["revendedores"]),
            "pedidos": int(row["pedidos"]),
            "itens": int(row["itens"]),
            "valor": round(float(row["valor"]), 2),
            "visitaram_unidade": int(row["visitaram_unidade"]),
            "receberam_casa": int(row["receberam_casa"]),
        }
        for row in result.iter_rows(named=True)
    ]


def calcular_por_segmento(df: pl.DataFrame, **filtros) -> List[Dict[str, Any]]:
    """Distribuição por segmentação (Papel)."""
    df = _aplicar_filtros(df, **filtros)
    if df.is_empty():
        return []
    result = (
        df.group_by("_segmento")
        .agg([
            pl.col(PED_COL_PESSOA).filter(pl.col(PED_COL_PESSOA) != "").n_unique().alias("revendedores"),
            pl.len().alias("pedidos"),
            pl.col("_itens").sum().alias("itens"),
            pl.col("_valor").sum().alias("valor"),
        ])
    )
    ordem = {s: i for i, s in enumerate(SEGMENTOS_ORDEM)}
    rows = [
        {
            "segmento": row["_segmento"],
            "revendedores": int(row["revendedores"]),
            "pedidos": int(row["pedidos"]),
            "itens": int(row["itens"]),
            "valor": round(float(row["valor"]), 2),
        }
        for row in result.iter_rows(named=True)
    ]
    rows.sort(key=lambda r: ordem.get(r["segmento"], 999))
    return rows


def calcular_composicao_cidades(df: pl.DataFrame, **filtros) -> List[Dict[str, Any]]:
    """
    Composição de segmentação (Papel) por cidade — base do modo "Composição"
    do mapa (rosquinha por cidade + cor do segmento predominante).

    Ignora o filtro de segmento (a composição é sempre sobre todos os papéis).
    """
    filtros = {**filtros, "segmento": None}
    df = _aplicar_filtros(df, **filtros)
    if df.is_empty():
        return []

    g = (
        df.group_by(["_cidade_moradia", "_segmento"])
        .agg(
            pl.col(PED_COL_PESSOA).filter(pl.col(PED_COL_PESSOA) != "").n_unique().alias("rev")
        )
    )
    ordem = {s: i for i, s in enumerate(SEGMENTOS_ORDEM)}
    por_cidade: Dict[str, Dict[str, int]] = {}
    for row in g.iter_rows(named=True):
        por_cidade.setdefault(row["_cidade_moradia"], {})[row["_segmento"]] = int(row["rev"])

    out: List[Dict[str, Any]] = []
    for cidade, segs in por_cidade.items():
        seg_list = sorted(
            [{"segmento": s, "revendedores": v} for s, v in segs.items()],
            key=lambda r: ordem.get(r["segmento"], 999),
        )
        total = sum(s["revendedores"] for s in seg_list)
        dominante = max(seg_list, key=lambda r: r["revendedores"])["segmento"] if seg_list else ""
        out.append({
            "cidade": cidade,
            "total": total,
            "dominante": dominante,
            "segmentos": seg_list,
        })
    out.sort(key=lambda r: r["total"], reverse=True)
    return out


def calcular_visitantes_unidade(df: pl.DataFrame, **filtros) -> List[Dict[str, Any]]:
    """Cidades cujos revendedores mais vão retirar na unidade (visitas)."""
    filtros = {**filtros, "tipo_visita": "unidade"}
    df = _aplicar_filtros(df, **filtros)
    if df.is_empty():
        return []
    result = (
        df.group_by(["_cidade_moradia", "_unidade"])
        .agg([
            pl.len().alias("visitas"),
            pl.col(PED_COL_PESSOA).filter(pl.col(PED_COL_PESSOA) != "").n_unique().alias("revendedores"),
        ])
        .sort("visitas", descending=True)
    )
    return [
        {
            "cidade": row["_cidade_moradia"],
            "unidade": row["_unidade"],
            "visitas": int(row["visitas"]),
            "revendedores": int(row["revendedores"]),
        }
        for row in result.iter_rows(named=True)
    ]


def calcular_detalhe_cidade(df: pl.DataFrame, cidade: str, **filtros) -> Dict[str, Any]:
    """Bairros + revendedores de uma cidade (drill-down do ranking)."""
    filtros = {**filtros, "cidade": cidade}
    df = _aplicar_filtros(df, **filtros)
    if df.is_empty():
        return {"bairros": [], "revendedores": []}

    bairros_df = (
        df.group_by("_bairro_moradia")
        .agg([
            pl.col(PED_COL_PESSOA).filter(pl.col(PED_COL_PESSOA) != "").n_unique().alias("revendedores"),
            pl.len().alias("pedidos"),
            pl.col("_itens").sum().alias("itens"),
            pl.col("_valor").sum().alias("valor"),
        ])
        .sort("revendedores", descending=True)
    )
    bairros = [
        {
            "bairro": row["_bairro_moradia"] or "Não informado",
            "revendedores": int(row["revendedores"]),
            "pedidos": int(row["pedidos"]),
            "itens": int(row["itens"]),
            "valor": round(float(row["valor"]), 2),
        }
        for row in bairros_df.iter_rows(named=True)
    ]

    # Revendedores da cidade (agrega por pessoa; pega o "melhor" segmento/setor).
    rev_df = (
        df.group_by([PED_COL_PESSOA])
        .agg([
            pl.col(PED_COL_NOME).first().alias("nome"),
            pl.col("_segmento").first().alias("segmento"),
            pl.col("_bairro_moradia").first().alias("bairro"),
            pl.col(PED_COL_COD_ESTRUTURA).first().alias("setor_cod"),
            pl.col(PED_COL_ESTRUTURA).first().alias("setor"),
            pl.col("_unidade").first().alias("unidade"),
            pl.col("_tipo_visita").last().alias("tipo_visita"),
            pl.col(PED_COL_TELEFONE).first().alias("telefone"),
            pl.len().alias("pedidos"),
            pl.col("_itens").sum().alias("itens"),
            pl.col("_valor").sum().alias("valor"),
        ])
        .sort("valor", descending=True)
    )
    revendedores = [
        {
            "codigo": row[PED_COL_PESSOA],
            "nome": row["nome"] or "—",
            "segmento": row["segmento"],
            "bairro": row["bairro"] or "Não informado",
            "setor_cod": row["setor_cod"],
            "setor": row["setor"],
            "unidade": row["unidade"],
            "tipo_visita": row["tipo_visita"],
            "telefone": row["telefone"],
            "pedidos": int(row["pedidos"]),
            "itens": int(row["itens"]),
            "valor": round(float(row["valor"]), 2),
        }
        for row in rev_df.iter_rows(named=True)
    ]

    return {"bairros": bairros, "revendedores": revendedores}


def obter_filtros(df: pl.DataFrame) -> Dict[str, Any]:
    """Opções para os dropdowns de filtro."""
    if df.is_empty():
        return {"segmentos": [], "unidades": [], "cidades": []}
    segs = df.select("_segmento").unique().to_series().to_list()
    ordem = {s: i for i, s in enumerate(SEGMENTOS_ORDEM)}
    segs = sorted([s for s in segs if s], key=lambda s: ordem.get(s, 999))
    unidades = (
        df.select(["_cod_unidade", "_unidade"]).unique()
        .filter(pl.col("_cod_unidade") != "")
        .sort("_unidade")
    )
    cidades = (
        df.select("_cidade_moradia").unique()
        .filter(pl.col("_cidade_moradia") != "Não informado")
        .sort("_cidade_moradia").to_series().to_list()
    )
    return {
        "segmentos": segs,
        "unidades": [
            {"codigo": r["_cod_unidade"], "nome": r["_unidade"]}
            for r in unidades.iter_rows(named=True)
        ],
        "cidades": cidades,
    }


def exportar_por_cidade(df: pl.DataFrame, **filtros) -> pl.DataFrame:
    return pl.DataFrame(calcular_por_cidade(df, **filtros))
