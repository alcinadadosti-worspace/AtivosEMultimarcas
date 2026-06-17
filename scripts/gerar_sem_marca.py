# -*- coding: utf-8 -*-
"""
Gera planilha 'Itens_Sem_Marca.xlsx' com os códigos vendidos que NÃO batem com
nenhuma marca do catálogo (entram como DESCONHECIDA e somem da contagem de
marcas). Une as vendas de geral.csv + bat.xlsx.

Colunas:
  CodigoProduto, CodigoProduto_Normalizado, NomeProduto, Qtde_Linhas_Venda,
  Marca_Sugerida (revisar), Marca_Confirmada (preencher), Observacao

Uso:
    python scripts/gerar_sem_marca.py
"""
import os
import sys
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl

from app.utils.normalizers import normalizar_sku
from app.services.produto import criar_indice_sku_em_memoria, buscar_sku_no_indice

DB_PATH = os.path.join("data", "produtos.db")
SAIDA = "Itens_Sem_Marca.xlsx"

# Palavras-chave de linha/sublinha -> marca do Grupo Boticário.
# Conservador: só sugere quando há uma linha reconhecível no nome.
LINHA_MARCA = [
    # Quem Disse Berenice
    ("QUEM DISSE", "Quem Disse Berenice"), ("QDB", "Quem Disse Berenice"),
    # O.U.I
    ("O.U.I", "O.U.I"), ("OUI ", "O.U.I"),
    # AuAmigos
    ("AUMIGOS", "AuAmigos"), ("AU MIGOS", "AuAmigos"), ("AUAMIGOS", "AuAmigos"),
    # Eudora (linhas)
    ("EUDORA", "Eudora"), ("INSTANCE", "Eudora"), ("NIINA", "Eudora"),
    ("SIAGE", "Eudora"), ("SIÀGE", "Eudora"), ("EUD ", "Eudora"), ("[EUD", "Eudora"),
    ("ROYAL", "Eudora"),
    # oBoticário (linhas)
    ("FLORATTA", "oBoticário"), ("LILY", "oBoticário"), ("NATIVA SPA", "oBoticário"),
    ("NSPA", "oBoticário"), ("NATIVA", "oBoticário"), ("CUIDE-SE BEM", "oBoticário"),
    ("CUIDE SE BEM", "oBoticário"), ("CBEM", "oBoticário"), ("COFFEE", "oBoticário"),
    ("MALBEC", "oBoticário"), ("EGEO", "oBoticário"), ("E:GEO", "oBoticário"),
    ("ZAAD", "oBoticário"), ("ARBO", "oBoticário"), ("MAKE B", "oBoticário"),
    ("MAKEB", "oBoticário"), ("INTENSE", "oBoticário"), ("GLAMOUR", "oBoticário"),
    ("ELYSEE", "oBoticário"), ("ELYSÉE", "oBoticário"), ("PORTINARI", "oBoticário"),
    ("HORUS", "oBoticário"), ("UOMINI", "oBoticário"), ("QUASAR", "oBoticário"),
    ("CHRONOS", "oBoticário"), ("BOTIK", "oBoticário"), ("ACQUA", "oBoticário"),
    ("BOTI ", "oBoticário"), ("DAMA DE OURO", "oBoticário"),
]

NAO_PRODUTO = ["CATALOGO", "CATÁLOGO", "SACOLA", "REVISTA", "FOLHETO", "AMOSTRA"]
COMBO_TOKENS = ["COMBO", "COMB ", "KIT", "PRESENTE", "CJ ", "CONJUNTO", "CX "]


def sugerir_marca(nome: str) -> str:
    if not nome:
        return ""
    up = nome.upper()
    for chave, marca in LINHA_MARCA:
        if chave in up:
            return marca
    return ""


def observacao(nome: str) -> str:
    if not nome:
        return ""
    up = nome.upper()
    if any(t in up for t in NAO_PRODUTO):
        return "Possível não-produto (catálogo/sacola/amostra)"
    if any(t in up for t in COMBO_TOKENS):
        return "Combo/Kit/Presente — confirmar marca (pode ser multimarca)"
    return ""


def ler_vendas():
    """Lê e empilha as vendas (CodigoProduto, NomeProduto) de geral.csv + bat.xlsx."""
    frames = []
    if os.path.exists("geral.csv"):
        g = pl.read_csv("geral.csv", separator="|", infer_schema_length=0)
        frames.append(g.select(["CodigoProduto", "NomeProduto", "Tipo"]))
    if os.path.exists("bat.xlsx"):
        b = pl.read_excel("bat.xlsx", infer_schema_length=0)
        frames.append(b.select(["CodigoProduto", "NomeProduto", "Tipo"]))
    if not frames:
        raise SystemExit("Nenhuma planilha de vendas encontrada (geral.csv / bat.xlsx).")
    df = pl.concat(frames, how="vertical_relaxed")
    return df.filter(pl.col("Tipo") == "Venda")


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        indice = criar_indice_sku_em_memoria(conn)
    finally:
        conn.close()

    df = ler_vendas()
    df_grp = df.group_by(["CodigoProduto", "NomeProduto"]).agg(
        pl.len().alias("Qtde_Linhas_Venda")
    )

    linhas = []
    for row in df_grp.iter_rows(named=True):
        marca, _nome, _motivo = buscar_sku_no_indice(row["CodigoProduto"], indice)
        if marca is not None:
            continue  # já tem marca, ignora
        nome = row["NomeProduto"] or ""
        linhas.append(
            {
                "CodigoProduto": row["CodigoProduto"],
                "CodigoProduto_Normalizado": normalizar_sku(row["CodigoProduto"]),
                "NomeProduto": nome,
                "Qtde_Linhas_Venda": row["Qtde_Linhas_Venda"],
                "Marca_Sugerida_revisar": sugerir_marca(nome),
                "Marca_Confirmada": "",
                "Observacao": observacao(nome),
            }
        )

    df_out = pl.DataFrame(linhas).sort("Qtde_Linhas_Venda", descending=True)

    import xlsxwriter

    wb = xlsxwriter.Workbook(SAIDA)
    ws = wb.add_worksheet("Sem_Marca")
    fmt_hdr = wb.add_format({"bold": True, "bg_color": "#22313F", "font_color": "white", "border": 1})
    fmt_fill = wb.add_format({"bg_color": "#FFF3CD"})  # coluna a preencher
    larguras = [16, 22, 50, 16, 24, 22, 40]
    for ci, col in enumerate(df_out.columns):
        ws.write(0, ci, col, fmt_hdr)
        ws.set_column(ci, ci, larguras[ci] if ci < len(larguras) else 18)
    col_conf = df_out.columns.index("Marca_Confirmada")
    for ri, rowvals in enumerate(df_out.iter_rows(named=False)):
        for ci, val in enumerate(rowvals):
            cell = "" if val is None else val
            ws.write(ri + 1, ci, cell, fmt_fill if ci == col_conf else None)
    ws.freeze_panes(1, 0)
    ws.autofilter(0, 0, df_out.height, len(df_out.columns) - 1)

    # aba de ajuda com as marcas válidas
    ws2 = wb.add_worksheet("Marcas_Validas")
    ws2.write(0, 0, "Marcas válidas (use exatamente assim)", fmt_hdr)
    for i, m in enumerate(["oBoticário", "Eudora", "Quem Disse Berenice", "O.U.I", "AuAmigos"]):
        ws2.write(i + 1, 0, m)
    wb.close()

    com_sug = df_out.filter(pl.col("Marca_Sugerida_revisar") != "").height
    print(f"Planilha gerada: {SAIDA}")
    print(f"  total sem marca: {df_out.height} | com sugestão: {com_sug} | sem sugestão: {df_out.height - com_sug}")


if __name__ == "__main__":
    main()
