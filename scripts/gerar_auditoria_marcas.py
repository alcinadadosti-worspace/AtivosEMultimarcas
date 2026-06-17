# -*- coding: utf-8 -*-
"""
Gera uma planilha de auditoria de marcas para análise manual.

Abas:
  - Resumo            : diagnóstico + contagens por marca
  - Catalogo_Produtos : tabela `produtos` (catálogo canônico, com marca)
  - Catalogo_IAF      : itens IAF (cabelos + make) com marca atual x padronizada
  - Marcas_Inconsist. : itens cuja marca foge do padrão (causa do multimarca torto)
  - Sem_Marca_Vendas  : códigos que aparecem nas vendas e NÃO batem com o catálogo

Uso:
    python scripts/gerar_auditoria_marcas.py
"""
import os
import sys
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl

from app.utils.normalizers import normalizar_marca, normalizar_sku
from app.services.produto import criar_indice_sku_em_memoria, buscar_sku_no_indice
from app.services.iaf import is_makeup_product

DB_PATH = os.path.join("data", "produtos.db")
VENDAS_CSV = "geral.csv"
SAIDA = "Auditoria_Marcas.xlsx"


def carregar_catalogo(conn):
    """Lê produtos + IAF e retorna (df_produtos, df_iaf, df_inconsistentes)."""
    cur = conn.cursor()

    # --- tabela produtos (catálogo canônico) ---
    cur.execute("SELECT sku, sku_normalizado, nome, marca FROM produtos ORDER BY marca, nome")
    prod_rows = cur.fetchall()
    df_produtos = pl.DataFrame(
        {
            "SKU": [r[0] for r in prod_rows],
            "SKU_Normalizado": [r[1] for r in prod_rows],
            "Nome": [r[2] for r in prod_rows],
            "Marca": [r[3] for r in prod_rows],
        }
    )

    # SKUs normalizados que já existem em `produtos` (têm prioridade no matching)
    skus_em_produtos = {r[1] for r in prod_rows}

    # --- tabelas IAF ---
    iaf_rows = []
    for tabela, origem in (("iaf_cabelos", "IAF Cabelos"), ("iaf_make", "IAF Make")):
        try:
            cur.execute(f"SELECT sku, sku_normalizado, descricao, marca FROM {tabela}")
        except sqlite3.OperationalError:
            continue
        for sku, skn, desc, marca in cur.fetchall():
            marca_padrao = normalizar_marca(marca)
            inconsistente = marca_padrao != (marca or "").strip()
            # Se o SKU também está em `produtos`, o catálogo canônico vence no
            # matching e a marca torta NÃO chega às vendas (impacto baixo).
            mascarado = skn in skus_em_produtos
            iaf_rows.append(
                {
                    "Origem": origem,
                    "SKU": sku,
                    "SKU_Normalizado": skn,
                    "Descricao": desc,
                    "Marca_Atual": marca,
                    "Marca_Padronizada": marca_padrao,
                    "Inconsistente": "SIM" if inconsistente else "NÃO",
                    "Tambem_no_Catalogo": "SIM" if mascarado else "NÃO",
                    "Impacto_no_Multimarca": (
                        ""
                        if not inconsistente
                        else ("Baixo (catálogo corrige)" if mascarado else "ALTO (chega torta nas vendas)")
                    ),
                }
            )

    df_iaf = pl.DataFrame(iaf_rows)
    df_incons = df_iaf.filter(pl.col("Inconsistente") == "SIM").sort(
        ["Impacto_no_Multimarca", "Marca_Atual"], descending=[True, False]
    )
    return df_produtos, df_iaf, df_incons


def itens_sem_marca_nas_vendas(conn):
    """Códigos de produto vendidos que NÃO batem com nenhuma marca do catálogo."""
    if not os.path.exists(VENDAS_CSV):
        return pl.DataFrame(
            {"CodigoProduto": [], "NomeProduto": [], "Qtde_Linhas_Venda": []}
        )

    indice = criar_indice_sku_em_memoria(conn)

    df = pl.read_csv(VENDAS_CSV, separator="|", infer_schema_length=0)
    if "Tipo" in df.columns:
        df = df.filter(pl.col("Tipo") == "Venda")

    # Contagem de linhas por (codigo, nome)
    df_grp = (
        df.group_by(["CodigoProduto", "NomeProduto"])
        .agg(pl.len().alias("Qtde_Linhas_Venda"))
    )

    sem_marca = []
    for row in df_grp.iter_rows(named=True):
        marca, _nome, _motivo = buscar_sku_no_indice(row["CodigoProduto"], indice)
        if marca is None:  # não encontrado -> DESCONHECIDA
            sem_marca.append(
                {
                    "CodigoProduto": row["CodigoProduto"],
                    "CodigoProduto_Normalizado": normalizar_sku(row["CodigoProduto"]),
                    "NomeProduto": row["NomeProduto"],
                    "Qtde_Linhas_Venda": row["Qtde_Linhas_Venda"],
                }
            )

    if not sem_marca:
        return pl.DataFrame(
            {
                "CodigoProduto": [],
                "CodigoProduto_Normalizado": [],
                "NomeProduto": [],
                "Qtde_Linhas_Venda": [],
            }
        )
    return pl.DataFrame(sem_marca).sort("Qtde_Linhas_Venda", descending=True)


def make_fora_da_lista_iaf(conn):
    """Itens de maquiagem que estão em `produtos` mas faltam na tabela oficial iaf_make.

    Hoje eles só contam como IAF Make pelo heurístico de nome (frágil). A coluna
    Heuristico_Pega indica se o nome atual é reconhecido automaticamente.
    """
    cur = conn.cursor()
    cur.execute("SELECT sku_normalizado FROM iaf_make")
    ja_no_iaf = {r[0] for r in cur.fetchall()}

    cur.execute("SELECT sku, sku_normalizado, nome, marca FROM produtos ORDER BY marca, nome")
    linhas = []
    for sku, skn, nome, marca in cur.fetchall():
        if skn in ja_no_iaf:
            continue
        if not is_makeup_product(nome):
            continue
        linhas.append(
            {
                "SKU": sku,
                "SKU_Normalizado": skn,
                "Nome": nome,
                "Marca": marca,
            }
        )
    return pl.DataFrame(linhas) if linhas else pl.DataFrame(
        {"SKU": [], "SKU_Normalizado": [], "Nome": [], "Marca": []}
    )


def montar_resumo(df_produtos, df_iaf, df_incons, df_sem_marca, df_make_fora=None):
    linhas = []
    linhas.append(("DIAGNÓSTICO", ""))
    linhas.append(("Total de itens no catálogo (produtos)", str(df_produtos.height)))
    linhas.append(("Total de itens IAF (cabelos + make)", str(df_iaf.height)))
    linhas.append(
        ("Itens IAF com marca FORA do padrão", str(df_incons.height))
    )
    alto = df_incons.filter(
        pl.col("Impacto_no_Multimarca").str.starts_with("ALTO")
    ).height if df_incons.height else 0
    linhas.append(("  -> destes, ALTO impacto (chegam tortos nas vendas)", str(alto)))
    linhas.append(
        ("Códigos vendidos SEM marca (não encontrados no catálogo)", str(df_sem_marca.height))
    )
    if df_make_fora is not None:
        linhas.append(
            ("Maquiagem em 'produtos' FORA da lista oficial iaf_make", str(df_make_fora.height))
        )
    linhas.append(("", ""))
    linhas.append(("CONTAGEM POR MARCA (catálogo produtos)", ""))
    cont = (
        df_produtos.group_by("Marca")
        .agg(pl.len().alias("qtd"))
        .sort("qtd", descending=True)
    )
    for r in cont.iter_rows(named=True):
        linhas.append((f"  {r['Marca']}", str(r["qtd"])))

    return pl.DataFrame(
        {"Indicador": [l[0] for l in linhas], "Valor": [l[1] for l in linhas]}
    )


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        df_produtos, df_iaf, df_incons = carregar_catalogo(conn)
        df_sem_marca = itens_sem_marca_nas_vendas(conn)
        df_make_fora = make_fora_da_lista_iaf(conn)
        df_resumo = montar_resumo(df_produtos, df_iaf, df_incons, df_sem_marca, df_make_fora)
    finally:
        conn.close()

    abas = {
        "Resumo": df_resumo,
        "Marcas_Inconsistentes": df_incons,
        "Make_Fora_Lista_IAF": df_make_fora,
        "Catalogo_Produtos": df_produtos,
        "Catalogo_IAF": df_iaf,
        "Sem_Marca_Vendas": df_sem_marca,
    }

    import xlsxwriter

    wb = xlsxwriter.Workbook(SAIDA, {"in_memory": False})
    fmt_hdr = wb.add_format({"bold": True, "bg_color": "#22313F", "font_color": "white", "border": 1})
    fmt_alto = wb.add_format({"bg_color": "#F9D6D5"})
    for nome_aba, df in abas.items():
        ws = wb.add_worksheet(nome_aba[:31])
        for ci, col in enumerate(df.columns):
            ws.write(0, ci, col, fmt_hdr)
            ws.set_column(ci, ci, max(12, min(45, len(col) + 4)))
        for ri, rowvals in enumerate(df.iter_rows(named=False)):
            destaque = (
                nome_aba == "Marcas_Inconsistentes"
                and "Impacto_no_Multimarca" in df.columns
                and str(rowvals[df.columns.index("Impacto_no_Multimarca")]).startswith("ALTO")
            )
            for ci, val in enumerate(rowvals):
                cell = "" if val is None else val
                if destaque:
                    ws.write(ri + 1, ci, cell, fmt_alto)
                else:
                    ws.write(ri + 1, ci, cell)
        ws.freeze_panes(1, 0)
        if df.height:
            ws.autofilter(0, 0, df.height, len(df.columns) - 1)
    wb.close()

    print(f"Planilha gerada: {SAIDA}")
    print(f"  produtos: {df_produtos.height} | IAF: {df_iaf.height} | "
          f"inconsistentes: {df_incons.height} | sem marca (vendas): {df_sem_marca.height}")


if __name__ == "__main__":
    main()
