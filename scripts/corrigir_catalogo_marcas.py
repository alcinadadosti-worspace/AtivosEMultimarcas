# -*- coding: utf-8 -*-
"""
Corrige o catálogo de produtos (data/produtos.db):

1. Padroniza marcas "tortas" (BOT/EUD/QDB/oBoticario/...) -> marca canônica,
   nas tabelas produtos, iaf_make e iaf_cabelos (idempotente).
2. Adiciona à tabela oficial iaf_make TODA maquiagem que está em `produtos`
   mas ainda não está no iaf_make (regra: todo item de maquiagem é IAF Make).

Faz backup do banco antes de qualquer alteração e roda tudo numa transação.

Uso:
    python scripts/corrigir_catalogo_marcas.py
"""
import os
import sys
import shutil
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.normalizers import normalizar_marca
from app.services.iaf import is_makeup_product

DB_PATH = os.path.join("data", "produtos.db")
BACKUP_PATH = os.path.join("data", "produtos.db.bak_pre_correcao_marcas")


def padronizar_marcas(conn):
    """Aplica normalizar_marca em produtos, iaf_make e iaf_cabelos."""
    cur = conn.cursor()
    total = 0
    detalhes = {}
    for tabela in ("produtos", "iaf_make", "iaf_cabelos"):
        try:
            cur.execute(f"SELECT id, marca FROM {tabela}")
        except sqlite3.OperationalError:
            continue
        rows = cur.fetchall()
        alterados = 0
        for _id, marca in rows:
            canonica = normalizar_marca(marca)
            if canonica != (marca or ""):
                cur.execute(
                    f"UPDATE {tabela} SET marca = ? WHERE id = ?", (canonica, _id)
                )
                alterados += 1
        detalhes[tabela] = alterados
        total += alterados
    return total, detalhes


def adicionar_maquiagens_ao_iaf(conn):
    """Insere em iaf_make toda maquiagem de `produtos` ausente da tabela."""
    cur = conn.cursor()
    cur.execute("SELECT sku_normalizado FROM iaf_make")
    ja_no_iaf = {r[0] for r in cur.fetchall()}

    cur.execute("SELECT sku, sku_normalizado, nome, marca FROM produtos")
    novos = []
    for sku, skn, nome, marca in cur.fetchall():
        if skn in ja_no_iaf:
            continue
        if not is_makeup_product(nome):
            continue
        novos.append((sku, skn, nome, normalizar_marca(marca)))

    cur.executemany(
        "INSERT INTO iaf_make (sku, sku_normalizado, descricao, marca) VALUES (?, ?, ?, ?)",
        novos,
    )
    # contagem por marca dos inseridos
    from collections import Counter
    por_marca = dict(Counter(n[3] for n in novos))
    return len(novos), por_marca


def main():
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"Banco não encontrado: {DB_PATH}")

    # Backup
    shutil.copy2(DB_PATH, BACKUP_PATH)
    print(f"[backup] {BACKUP_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM iaf_make")
        antes_iaf = cur.fetchone()[0]

        # 1) padronizar marcas
        total_marcas, det_marcas = padronizar_marcas(conn)

        # 2) adicionar maquiagens
        total_novos, por_marca = adicionar_maquiagens_ao_iaf(conn)

        conn.commit()

        cur.execute("SELECT COUNT(*) FROM iaf_make")
        depois_iaf = cur.fetchone()[0]

        print()
        print("=== 1) Padronização de marcas (linhas alteradas) ===")
        for t, q in det_marcas.items():
            print(f"  {t}: {q}")
        print(f"  TOTAL: {total_marcas}")
        print()
        print("=== 2) Maquiagens adicionadas ao iaf_make ===")
        print(f"  inseridos: {total_novos}  (iaf_make: {antes_iaf} -> {depois_iaf})")
        for m, q in sorted(por_marca.items(), key=lambda x: -x[1]):
            print(f"    {m}: {q}")

        # Verificações finais
        print()
        print("=== Verificação ===")
        cur.execute(
            "SELECT marca, COUNT(*) FROM iaf_make GROUP BY marca ORDER BY 2 DESC"
        )
        marcas_iaf = cur.fetchall()
        aliases_restantes = [
            (m, q) for m, q in marcas_iaf if normalizar_marca(m) != m
        ]
        print("  Marcas no iaf_make agora:")
        for m, q in marcas_iaf:
            print(f"    {m}: {q}")
        print(f"  Marcas ainda fora do padrão: {len(aliases_restantes)}")
    except Exception:
        conn.rollback()
        print("[ERRO] rollback aplicado — banco não foi alterado.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
