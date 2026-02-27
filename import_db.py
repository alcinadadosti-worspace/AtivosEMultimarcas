"""
Import script to convert Excel spreadsheets to SQLite database.

This script reads:
- estoqueplanilha.xlsx: Main product database
- cabelos_iaf.xlsx: IAF Cabelos products (optional)
- make_iaf.xlsx: IAF Make products (optional)

And populates the SQLite database with normalized data.
"""
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

import sqlite3
import polars as pl

from app.config import DATABASE_PATH, DATA_DIR
from app.utils.normalizers import normalizar_sku, normalizar_marca


def import_produtos(conn: sqlite3.Connection, filepath: Path) -> int:
    """
    Import products from Excel file.

    Args:
        conn: SQLite connection
        filepath: Path to estoqueplanilha.xlsx

    Returns:
        Number of products imported
    """
    if not filepath.exists():
        print(f"[WARN] Product file not found: {filepath}")
        return 0

    print(f"[INFO] Reading products from {filepath}...")
    df = pl.read_excel(filepath, infer_schema_length=0)

    print(f"[INFO] Found {len(df)} rows, {len(df.columns)} columns")
    print(f"[INFO] Columns: {df.columns}")

    # Find the SKU column (might be named differently)
    sku_col = None
    nome_col = None
    marca_col = None

    for col in df.columns:
        col_lower = col.lower().strip()
        if col_lower in ['sku', 'codigo', 'código', 'cod', 'codigoproduto']:
            sku_col = col
        elif col_lower in ['nome', 'nomeproduto', 'descricao', 'descrição', 'produto']:
            nome_col = col
        elif col_lower in ['marca']:
            marca_col = col

    if not sku_col:
        # Try first column as SKU
        sku_col = df.columns[0]
        print(f"[WARN] SKU column not found, using first column: {sku_col}")

    if not nome_col:
        # Try second column as name
        nome_col = df.columns[1] if len(df.columns) > 1 else None
        print(f"[WARN] Nome column not found, using: {nome_col}")

    if not marca_col:
        # Try third column as brand
        marca_col = df.columns[2] if len(df.columns) > 2 else None
        print(f"[WARN] Marca column not found, using: {marca_col}")

    cursor = conn.cursor()

    # Clear existing data
    cursor.execute("DELETE FROM produtos")

    imported = 0
    skipped = 0

    for row in df.iter_rows(named=True):
        sku = str(row.get(sku_col, "")) if row.get(sku_col) is not None else ""
        nome = str(row.get(nome_col, "")) if nome_col and row.get(nome_col) is not None else ""
        marca = str(row.get(marca_col, "")) if marca_col and row.get(marca_col) is not None else ""

        # Normalize
        sku_norm = normalizar_sku(sku)
        marca_norm = normalizar_marca(marca)

        if not sku_norm:
            skipped += 1
            continue

        try:
            cursor.execute("""
                INSERT INTO produtos (sku, sku_normalizado, nome, marca)
                VALUES (?, ?, ?, ?)
            """, (sku, sku_norm, nome, marca_norm))
            imported += 1
        except sqlite3.IntegrityError:
            # Duplicate SKU
            skipped += 1

    conn.commit()
    print(f"[INFO] Imported {imported} products, skipped {skipped}")
    return imported


def import_iaf(conn: sqlite3.Connection, filepath: Path, table_name: str) -> int:
    """
    Import IAF products from Excel file.

    Args:
        conn: SQLite connection
        filepath: Path to IAF Excel file
        table_name: Either 'iaf_cabelos' or 'iaf_make'

    Returns:
        Number of products imported
    """
    if not filepath.exists():
        print(f"[WARN] IAF file not found: {filepath}")
        return 0

    print(f"[INFO] Reading IAF from {filepath}...")
    df = pl.read_excel(filepath, infer_schema_length=0)

    print(f"[INFO] Found {len(df)} rows")

    # Find columns
    sku_col = None
    desc_col = None
    marca_col = None

    for col in df.columns:
        col_lower = col.lower().strip()
        if col_lower in ['sku', 'codigo', 'código', 'cod']:
            sku_col = col
        elif col_lower in ['descricao', 'descrição', 'nome', 'produto']:
            desc_col = col
        elif col_lower in ['marca']:
            marca_col = col

    if not sku_col:
        sku_col = df.columns[0]
    if not desc_col and len(df.columns) > 1:
        desc_col = df.columns[1]
    if not marca_col and len(df.columns) > 2:
        marca_col = df.columns[2]

    cursor = conn.cursor()

    # Clear existing data
    cursor.execute(f"DELETE FROM {table_name}")

    imported = 0

    for row in df.iter_rows(named=True):
        sku = str(row.get(sku_col, "")) if row.get(sku_col) is not None else ""
        desc = str(row.get(desc_col, "")) if desc_col and row.get(desc_col) is not None else ""
        marca = str(row.get(marca_col, "")) if marca_col and row.get(marca_col) is not None else ""

        sku_norm = normalizar_sku(sku)
        marca_norm = normalizar_marca(marca)

        if not sku_norm:
            continue

        cursor.execute(f"""
            INSERT INTO {table_name} (sku, sku_normalizado, descricao, marca)
            VALUES (?, ?, ?, ?)
        """, (sku, sku_norm, desc, marca_norm))
        imported += 1

    conn.commit()
    print(f"[INFO] Imported {imported} IAF items to {table_name}")
    return imported


def main():
    """Main import function."""
    print("=" * 60)
    print("Multimarks Analytics - Database Import")
    print("=" * 60)

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Connect to database
    print(f"\n[INFO] Database path: {DATABASE_PATH}")
    conn = sqlite3.connect(DATABASE_PATH)

    # Create schema
    print("[INFO] Creating database schema...")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS produtos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            sku_normalizado TEXT NOT NULL UNIQUE,
            nome TEXT NOT NULL,
            marca TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS iaf_cabelos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            sku_normalizado TEXT NOT NULL,
            descricao TEXT NOT NULL,
            marca TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS iaf_make (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            sku_normalizado TEXT NOT NULL,
            descricao TEXT NOT NULL,
            marca TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_produtos_sku_norm ON produtos(sku_normalizado)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_produtos_marca ON produtos(marca)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_produtos_sku ON produtos(sku)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_iaf_cabelos_sku ON iaf_cabelos(sku_normalizado)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_iaf_make_sku ON iaf_make(sku_normalizado)")

    conn.commit()

    # Import products
    print("\n--- Importing Products ---")
    produtos_file = DATA_DIR / "estoqueplanilha.xlsx"
    import_produtos(conn, produtos_file)

    # Import IAF files (optional)
    print("\n--- Importing IAF Cabelos ---")
    iaf_cabelos_file = DATA_DIR / "cabelos_iaf.xlsx"
    import_iaf(conn, iaf_cabelos_file, "iaf_cabelos")

    print("\n--- Importing IAF Make ---")
    iaf_make_file = DATA_DIR / "make_iaf.xlsx"
    import_iaf(conn, iaf_make_file, "iaf_make")

    # Show statistics
    print("\n--- Database Statistics ---")
    cursor.execute("SELECT COUNT(*) FROM produtos")
    print(f"Total products: {cursor.fetchone()[0]}")

    cursor.execute("SELECT marca, COUNT(*) FROM produtos GROUP BY marca ORDER BY COUNT(*) DESC")
    print("\nProducts by brand:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")

    cursor.execute("SELECT COUNT(*) FROM iaf_cabelos")
    print(f"\nIAF Cabelos: {cursor.fetchone()[0]}")

    cursor.execute("SELECT COUNT(*) FROM iaf_make")
    print(f"IAF Make: {cursor.fetchone()[0]}")

    conn.close()
    print("\n" + "=" * 60)
    print("Import completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
