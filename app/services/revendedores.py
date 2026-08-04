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


def _ordem_ciclo(c: str):
    """Ciclo vem como 'MM/AAAA'. Ordem alfabética quebra na virada do ano
    ('01/2027' viria antes de '11/2026'), e a timeline depende da ordem certa."""
    p = c.split("/")
    if len(p) == 2 and p[0].strip().isdigit() and p[1].strip().isdigit():
        return (1, int(p[1]), int(p[0]), c)
    return (0, 0, 0, c)   # formato inesperado -> alfabético, antes dos demais


def ciclos_do_arquivo(df_ped: pl.DataFrame) -> List[str]:
    vals = (
        df_ped.select(pl.col(PED_COL_CICLO).cast(pl.Utf8).str.strip_chars())
        .to_series().drop_nulls().unique().to_list()
    )
    return sorted((c for c in vals if c), key=_ordem_ciclo)


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
# Alerta de inatividade — clientes há X ciclos sem comprar
#
# A contagem sai do ARQUIVO DE PEDIDOS, não do CiclosInatividade da base: a
# coluna da base é uma foto do dia em que a planilha foi extraída e fica para
# trás assim que um ciclo novo entra no arquivo de pedidos (base do ciclo 10 +
# pedidos até o 11 = filtro "exatamente 3" mostrando 4 quadrados vermelhos).
# Contando pelo arquivo, o filtro e a timeline são a mesma conta e não podem
# divergir. Sem arquivo de pedidos na sessão, cai para a coluna da base.
# ─────────────────────────────────────────────────────────────────────────────

def _marcos(ciclos: List[str], idx: Dict[str, int], comprou: set, ciclo_primeiro: str):
    """(ini, ult) da janela do arquivo:
      ini — 1º ciclo em que o cliente já era revendedor. Começou antes da
            janela -> 0; começou depois dela -> len(ciclos), e aí nenhum ciclo
            conta (senão um cadastro novo apareceria como "abandonando").
      ult — último ciclo em que comprou (-1 se não comprou em nenhum).
    """
    if not ciclo_primeiro:
        ini = 0
    elif ciclo_primeiro in idx:
        ini = idx[ciclo_primeiro]
    else:
        ini = len(ciclos) if _ordem_ciclo(ciclo_primeiro) > _ordem_ciclo(ciclos[-1]) else 0
    ult = max((idx[c] for c in comprou if c in idx), default=-1)
    return ini, ult


def _estados_ciclo(ciclos: List[str], comprou: set, ciclo_primeiro: str) -> List[Dict[str, str]]:
    """Situação do cliente em cada ciclo do arquivo:
      comprou      — fez pedido no ciclo
      antes        — ainda não era revendedor (anterior à 1ª compra); não conta
      gap          — sem comprar daí até hoje; é isto que a contagem soma
      intercalado  — pulou o ciclo, mas voltou a comprar depois; não conta
    """
    if not ciclos:
        return []
    idx = {c: i for i, c in enumerate(ciclos)}
    ini, ult = _marcos(ciclos, idx, comprou, ciclo_primeiro)
    out = []
    for i, c in enumerate(ciclos):
        if c in comprou:
            e = "comprou"
        elif i < ini:
            e = "antes"
        elif i > ult:
            e = "gap"
        else:
            e = "intercalado"
        out.append({"ciclo": c, "estado": e})
    return out


def _gaps_do_arquivo(df_rev: pl.DataFrame, df_ped) -> Dict[str, int]:
    """Por revendedor: quantos ciclos seguidos sem comprar até o ciclo mais
    recente do arquivo — exatamente o número de quadrados vermelhos da timeline."""
    ciclos = ciclos_do_arquivo(df_ped)
    if not ciclos:
        return {}
    idx = {c: i for i, c in enumerate(ciclos)}
    compras = _ciclos_comprados_por_cod(df_ped)
    n = len(ciclos)
    gaps: Dict[str, int] = {}
    for r in df_rev.select(["_cod", "_ciclo_primeiro"]).iter_rows(named=True):
        comprou = compras.get(r["_cod"], set())
        ini, ult = _marcos(ciclos, idx, comprou, r["_ciclo_primeiro"])
        gaps[r["_cod"]] = n - 1 - max(ult, ini - 1)   # = nº de ciclos marcados "gap"
    return gaps


def _com_inatividade(df_rev: pl.DataFrame, df_ped=None) -> pl.DataFrame:
    """Coluna `_inat` — os ciclos sem comprar que o painel filtra e exibe.
    `_inatividade` (base) segue disponível como referência."""
    gaps = _gaps_do_arquivo(df_rev, df_ped) if df_ped is not None else {}
    if not gaps:
        return df_rev.with_columns(pl.col("_inatividade").alias("_inat"))
    mapa = pl.DataFrame(
        {"_cod": list(gaps.keys()), "_inat": list(gaps.values())},
        schema={"_cod": pl.Utf8, "_inat": pl.Int64},
    )
    return df_rev.join(mapa, on="_cod", how="left").with_columns(
        pl.col("_inat").fill_null(pl.col("_inatividade"))
    )


def _filtro_alerta(df_rev: pl.DataFrame, unidade, min_c: int, max_c: int, segmento=None, setor=None, df_ped=None) -> pl.DataFrame:
    d = _com_inatividade(df_rev, df_ped)
    d = _filtrar_unidade(d, unidade)
    if segmento:
        d = d.filter(pl.col("_segmento") == segmento)
    if setor:
        d = d.filter(pl.col("_setor_cod") == str(setor))   # código único por setor (nome pode repetir entre unidades)
    return d.filter((pl.col("_inat") >= min_c) & (pl.col("_inat") <= max_c))


def alerta_resumo(df_rev, unidade=None, min_c: int = 5, max_c: int = 7, segmento=None, setor=None, df_ped=None) -> Dict[str, Any]:
    d = _filtro_alerta(df_rev, unidade, min_c, max_c, segmento, setor, df_ped)
    ciclos = ciclos_do_arquivo(df_ped) if df_ped is not None else []
    return {
        "total": d.height,
        "por_ciclo": {
            int(c): int(d.filter(pl.col("_inat") == c).height)
            for c in range(min_c, max_c + 1)
        },
        "cidades": d.select("_cidade").filter(pl.col("_cidade") != "").n_unique(),
        "min": min_c, "max": max_c,
        # de onde saiu a contagem, pra tela poder dizer isso ao usuário
        "fonte": "pedidos" if ciclos else "base",
        "ciclo_ref": ciclos[-1] if ciclos else "",
        "janela": len(ciclos),
    }


def alerta_por_cidade(df_rev, unidade=None, min_c: int = 5, max_c: int = 7, segmento=None, setor=None, df_ped=None) -> List[Dict[str, Any]]:
    """Quantidade de clientes em alerta por cidade de cadastro (residencial)."""
    d = _filtro_alerta(df_rev, unidade, min_c, max_c, segmento, setor, df_ped)
    if d.is_empty():
        return []
    d = d.with_columns(
        pl.when(pl.col("_cidade") == "").then(pl.lit("Não informado")).otherwise(pl.col("_cidade")).alias("_cid")
    )
    g = (
        d.group_by("_cid").agg([
            pl.len().alias("total"),
            pl.col("_inat").max().alias("pior"),
        ]).sort("total", descending=True)
    )
    return [
        {"cidade": r["_cid"], "total": int(r["total"]), "pior": int(r["pior"])}
        for r in g.iter_rows(named=True)
    ]


def _ciclos_comprados_por_cod(df_ped) -> Dict[str, set]:
    """Do arquivo de pedidos: por revendedor (código normalizado), o conjunto de
    ciclos em que comprou. Usado para o histórico (comprou/não por ciclo)."""
    d = (
        df_ped.with_columns([
            _norm_cod(PED_COL_PESSOA).alias("_cod"),
            pl.col(PED_COL_CICLO).cast(pl.Utf8).fill_null("").str.strip_chars().alias("_ciclo"),
        ]).filter((pl.col("_cod") != "") & (pl.col("_ciclo") != ""))
        .group_by("_cod").agg(pl.col("_ciclo").unique().alias("ciclos"))
    )
    return {r["_cod"]: set(r["ciclos"]) for r in d.iter_rows(named=True)}


def alerta_detalhe_cidade(df_rev, cidade: str, df_ped=None, unidade=None, min_c: int = 5, max_c: int = 7, segmento=None, setor=None) -> Dict[str, Any]:
    """Clientes em alerta de uma cidade + histórico de compras por ciclo (do
    arquivo de pedidos), pra distinguir inatividade intercalada de consecutiva."""
    d = _filtro_alerta(df_rev, unidade, min_c, max_c, segmento, setor, df_ped)
    alvo = cidade.strip().lower()
    if alvo == "não informado":   # cidade agrupada no ranking = _cidade vazia
        d = d.filter(pl.col("_cidade") == "")
    else:
        d = d.filter(pl.col("_cidade").str.to_lowercase() == alvo)
    d = d.sort("_inat", descending=True)

    ciclos = ciclos_do_arquivo(df_ped) if df_ped is not None else []
    compras = _ciclos_comprados_por_cod(df_ped) if df_ped is not None else {}

    clientes = []
    for r in d.iter_rows(named=True):
        comprou = compras.get(r["_cod"], set())
        clientes.append({
            "codigo": r["_cod"],
            "nome": r["_nome"] or "—",
            "inatividade": int(r["_inat"]),          # do arquivo — bate com a timeline
            "inatividade_base": int(r["_inatividade"]),   # foto da base, só referência
            "situacao": r["_situacao"],
            "segmento": r["_segmento"],
            "setor": r["_setor"],
            "unidade": r["_unidade"],
            "telefone": r["_telefone"],
            "ciclo_primeiro": r["_ciclo_primeiro"],
            # histórico: estado do cliente em cada ciclo do arquivo
            "historico": _estados_ciclo(ciclos, comprou, r["_ciclo_primeiro"]),
        })
    return {"clientes": clientes, "ciclos": ciclos}


def alerta_lista(df_rev, unidade=None, min_c: int = 5, max_c: int = 7, segmento=None, setor=None, df_ped=None) -> List[Dict[str, Any]]:
    """Todos os clientes em alerta, cidade a cidade, numa passada só (export)."""
    d = _filtro_alerta(df_rev, unidade, min_c, max_c, segmento, setor, df_ped)
    if d.is_empty():
        return []
    d = d.sort(["_cidade", "_inat"], descending=[False, True])
    return [
        {
            "cidade": r["_cidade"] or "Não informado",
            "codigo": r["_cod"],
            "nome": r["_nome"] or "—",
            "inatividade": int(r["_inat"]),
            "inatividade_base": int(r["_inatividade"]),
            "situacao": r["_situacao"],
            "segmento": r["_segmento"],
            "setor": r["_setor"],
            "unidade": r["_unidade"],
            "telefone": r["_telefone"],
            "ciclo_primeiro": r["_ciclo_primeiro"],
        }
        for r in d.iter_rows(named=True)
    ]


def obter_unidades(df_rev: pl.DataFrame) -> List[Dict[str, str]]:
    u = df_rev.select(["_cod_unidade", "_unidade"]).unique().sort("_unidade")
    return [{"codigo": r["_cod_unidade"], "nome": r["_unidade"]} for r in u.iter_rows(named=True)]


def obter_segmentos_base(df_rev: pl.DataFrame) -> List[str]:
    """Segmentos (Papel) presentes na base, na ordem canônica."""
    from app.services.pedidos import SEGMENTOS_ORDEM
    segs = df_rev.select("_segmento").unique().to_series().to_list()
    ordem = {s: i for i, s in enumerate(SEGMENTOS_ORDEM)}
    return sorted([s for s in segs if s], key=lambda s: ordem.get(s, 999))


def obter_setores_base(df_rev: pl.DataFrame) -> List[Dict[str, str]]:
    """Setores (EstruturaComercial) da base, com o código da unidade a que
    pertencem — o front usa isso pra mostrar só os setores da unidade escolhida.
    Filtra por `_setor_cod` (único); o nome pode repetir entre unidades (ex.: SETOR PADRÃO)."""
    s = (
        df_rev.select(["_setor_cod", "_setor", "_cod_unidade"])
        .filter(pl.col("_setor") != "")
        .unique(subset=["_setor_cod"], keep="first")
        .sort(["_cod_unidade", "_setor"])
    )
    return [
        {"cod": r["_setor_cod"], "nome": r["_setor"], "unidade": r["_cod_unidade"]}
        for r in s.iter_rows(named=True)
    ]
