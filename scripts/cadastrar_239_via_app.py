# -*- coding: utf-8 -*-
"""
Cadastra os 239 itens da planilha Itens_Sem_Marca_Corrigidos.xlsx usando a
PRÓPRIA função do app (app.api.routes.cadastrar_produtos_lote) — assim testamos
a função de cadastro por marca e, de quebra, ela grava em data/produtos.db E em
data/estoqueplanilha.xlsx (fonte versionada).

Backups antes de tudo.
"""
import os
import sys
import shutil
import sqlite3
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openpyxl
from app.api.routes import cadastrar_produtos_lote

DB = os.path.join("data", "produtos.db")
PLAN = "Itens_Sem_Marca_Corrigidos.xlsx"


def carregar_239():
    wb = openpyxl.load_workbook(PLAN, read_only=True)
    ws = wb["Sem_Marca"]
    itens = []
    for r in ws.iter_rows(min_row=2, values_only=True):
        cod, _norm, nome, _q, _sug, conf, _obs = (tuple(r) + (None,) * 7)[:7]
        if cod is None and nome is None:
            continue
        itens.append({"sku": str(cod), "nome": str(nome) if nome else "", "marca": str(conf).strip() if conf else ""})
    return itens


def main():
    # Backups
    for src in [DB, os.path.join("data", "estoqueplanilha.xlsx")]:
        dst = src + ".bak_pre_cadastro239"
        shutil.copy2(src, dst)
        print(f"[backup] {dst}")

    itens = carregar_239()
    print(f"[info] {len(itens)} itens para cadastrar")

    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM produtos")
    antes = cur.fetchone()[0]

    # sessão fake (sem dados carregados -> _atualizar_sessao retorna 0)
    session = ("teste-cadastro", {"df_vendas": None})

    resultado = asyncio.run(cadastrar_produtos_lote(produtos=itens, conn=conn, session=session))

    cur.execute("SELECT COUNT(*) FROM produtos")
    depois = cur.fetchone()[0]
    conn.close()

    print()
    print("=== RESULTADO DA FUNÇÃO DO APP (cadastrar_produtos_lote) ===")
    print(f"  success: {resultado.get('success')}")
    print(f"  cadastrados: {resultado.get('cadastrados')}")
    print(f"  total_erros: {resultado.get('total_erros')}")
    for e in resultado.get("erros", [])[:15]:
        print(f"    erro: {e}")
    print(f"  produtos no banco: {antes} -> {depois}")


if __name__ == "__main__":
    main()
