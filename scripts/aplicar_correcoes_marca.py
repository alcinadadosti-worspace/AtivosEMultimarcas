# -*- coding: utf-8 -*-
"""
Aplica em data/produtos.db as correções de marca que o usuário fez na planilha
CatProd.xlsx (coluna Marca), para os SKUs cuja marca diverge do banco.

Exceção manual confirmada pelo usuário:
  - 88632 (OUI LOC DES HID CPO MAD V2 400ml) -> O.U.I  (não Eudora)

Faz backup antes e roda numa transação.
"""
import os
import sys
import shutil
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openpyxl

DB_PATH = os.path.join("data", "produtos.db")
PLANILHA = "CatProd.xlsx"
BACKUP_PATH = os.path.join("data", "produtos.db.bak_pre_correcao_planilha")

# Override manual: SKU_normalizado -> marca correta (confirmado pelo usuário)
OVERRIDES = {
    "88632": "O.U.I",
}


def main():
    # Marcas da planilha (ajustada pelo usuário)
    wb = openpyxl.load_workbook(PLANILHA, read_only=True)
    ws = wb["Planilha1"]
    plan = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        sku, skn, nome, marca = (tuple(row) + (None,) * 4)[:4]
        if skn is None:
            continue
        plan[str(skn).strip()] = (str(marca).strip() if marca else "")

    shutil.copy2(DB_PATH, BACKUP_PATH)
    print(f"[backup] {BACKUP_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT sku_normalizado, nome, marca FROM produtos")
        db = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

        mudancas = []
        for skn, (nome, marca_db) in db.items():
            marca_nova = OVERRIDES.get(skn, plan.get(skn))
            if marca_nova is None:
                continue
            if (marca_db or "").strip() != marca_nova:
                mudancas.append((skn, nome, marca_db, marca_nova))

        for skn, nome, _antiga, nova in mudancas:
            cur.execute(
                "UPDATE produtos SET marca = ?, updated_at = CURRENT_TIMESTAMP WHERE sku_normalizado = ?",
                (nova, skn),
            )
        conn.commit()

        print(f"\n=== {len(mudancas)} marcas atualizadas ===")
        for skn, nome, antiga, nova in sorted(mudancas, key=lambda x: x[3]):
            flag = "  <- OVERRIDE" if skn in OVERRIDES else ""
            print(f"  {skn:>7} | {str(nome)[:40]:40} | {antiga:12} -> {nova}{flag}")

        # Verificação
        print("\n=== Conferência (deve bater com o esperado) ===")
        for skn in [s for s, _, _, _ in mudancas]:
            cur.execute("SELECT marca FROM produtos WHERE sku_normalizado = ?", (skn,))
            pass
        print("  88632 agora:", end=" ")
        cur.execute("SELECT marca FROM produtos WHERE sku_normalizado = '88632'")
        print(cur.fetchone()[0])

        cur.execute("SELECT marca, COUNT(*) FROM produtos GROUP BY marca ORDER BY 2 DESC")
        print("  Distribuição por marca (produtos):")
        for m, q in cur.fetchall():
            print(f"    {m}: {q}")
    except Exception:
        conn.rollback()
        print("[ERRO] rollback — banco não alterado.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
