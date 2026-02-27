"""
SQLite database connection and management.
"""
import sqlite3
from contextlib import contextmanager
from typing import Generator

from app.config import DATABASE_PATH


def get_connection() -> sqlite3.Connection:
    """
    Get a new SQLite database connection.

    Returns:
        SQLite connection with Row factory enabled
    """
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db() -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager for database connections.

    Automatically closes connection when done.

    Usage:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM produtos")
    """
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def init_database() -> None:
    """
    Initialize database schema.

    Creates all required tables and indexes if they don't exist.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Products table
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

        # IAF Cabelos table
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

        # IAF Make table
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

        # Indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_produtos_sku_norm
            ON produtos(sku_normalizado)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_produtos_marca
            ON produtos(marca)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_produtos_sku
            ON produtos(sku)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_iaf_cabelos_sku
            ON iaf_cabelos(sku_normalizado)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_iaf_make_sku
            ON iaf_make(sku_normalizado)
        """)

        conn.commit()


def get_db_stats() -> dict:
    """
    Get database statistics.

    Returns:
        Dictionary with table counts and database info
    """
    with get_db() as conn:
        cursor = conn.cursor()

        stats = {}

        # Count products
        cursor.execute("SELECT COUNT(*) FROM produtos")
        stats["total_produtos"] = cursor.fetchone()[0]

        # Count by brand
        cursor.execute("""
            SELECT marca, COUNT(*) as qtde
            FROM produtos
            GROUP BY marca
            ORDER BY qtde DESC
        """)
        stats["produtos_por_marca"] = {row[0]: row[1] for row in cursor.fetchall()}

        # Count IAF
        cursor.execute("SELECT COUNT(*) FROM iaf_cabelos")
        stats["total_iaf_cabelos"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM iaf_make")
        stats["total_iaf_make"] = cursor.fetchone()[0]

        return stats
