"""
Product service with robust SKU matching.

This module provides functions to search products in the database
with multiple matching strategies to handle SKU format variations.
"""
import sqlite3
from typing import Optional, Dict, Tuple

from app.config import (
    MOTIVO_MATCH_EXATO,
    MOTIVO_MATCH_COM_ZERO,
    MOTIVO_MATCH_SEM_ZERO,
    MOTIVO_NAO_ENCONTRADO,
)
from app.utils.normalizers import normalizar_sku


def buscar_produto(
    codigo_produto: str,
    conn: sqlite3.Connection
) -> Optional[Dict[str, str]]:
    """
    Search for a product in the SQLite database with robust matching strategy.

    SEARCH STRATEGY (in order):
    1. Exact match by normalized SKU
    2. If code has 4 digits -> try with leading zero (01234)
    3. If code has 5+ digits and starts with 0 -> try without zero (1234)

    This strategy solves the common problem where:
    - DB has SKU "01234" but sale comes as "1234"
    - DB has SKU "1234" but sale comes as "01234"

    Args:
        codigo_produto: Product code (normalized or not)
        conn: Active SQLite connection

    Returns:
        Dict with {sku, nome, marca, motivo} or None if not found
        - motivo: MATCH_EXATO, MATCH_COM_ZERO, MATCH_SEM_ZERO, or NAO_ENCONTRADO

    Examples:
        >>> buscar_produto("1234", conn)
        {"sku": "01234", "nome": "Produto X", "marca": "oBoticário", "motivo": "MATCH_COM_ZERO"}
    """
    # Normalize input code
    sku_norm = normalizar_sku(codigo_produto)

    if not sku_norm:
        return None

    cursor = conn.cursor()

    # 1. ATTEMPT: Exact match
    cursor.execute(
        "SELECT sku, nome, marca FROM produtos WHERE sku_normalizado = ?",
        (sku_norm,)
    )
    row = cursor.fetchone()
    if row:
        return {
            "sku": row[0],
            "nome": row[1],
            "marca": row[2],
            "motivo": MOTIVO_MATCH_EXATO
        }

    # 2. ATTEMPT: With leading zero (4 digits -> 5 digits)
    if len(sku_norm) == 4:
        sku_com_zero = '0' + sku_norm
        cursor.execute(
            "SELECT sku, nome, marca FROM produtos WHERE sku_normalizado = ?",
            (sku_com_zero,)
        )
        row = cursor.fetchone()
        if row:
            return {
                "sku": row[0],
                "nome": row[1],
                "marca": row[2],
                "motivo": MOTIVO_MATCH_COM_ZERO
            }

    # 3. ATTEMPT: Without leading zero (5+ digits -> fewer digits)
    if len(sku_norm) >= 5 and sku_norm.startswith('0'):
        sku_sem_zero = sku_norm.lstrip('0') or '0'  # Preserve at least one '0'
        cursor.execute(
            "SELECT sku, nome, marca FROM produtos WHERE sku_normalizado = ?",
            (sku_sem_zero,)
        )
        row = cursor.fetchone()
        if row:
            return {
                "sku": row[0],
                "nome": row[1],
                "marca": row[2],
                "motivo": MOTIVO_MATCH_SEM_ZERO
            }

    # Not found in any attempt
    return None


def criar_indice_sku_em_memoria(conn: sqlite3.Connection) -> Dict[str, Dict]:
    """
    Create an in-memory index for even faster searches.

    Useful when there are many consecutive searches (spreadsheet processing).
    The index already includes variations with/without zero.

    IMPORTANT: Also includes products from IAF tables (iaf_cabelos, iaf_make)
    so that IAF-only products are also found during matching.

    Args:
        conn: SQLite connection

    Returns:
        Dict mapping sku_normalizado -> {sku, nome, marca}
    """
    cursor = conn.cursor()
    indice = {}

    def adicionar_ao_indice(sku_original: str, sku_norm: str, nome: str, marca: str, is_iaf: bool = False):
        """Helper to add product and its variations to index."""
        # Main index (don't overwrite if already exists from produtos table)
        if sku_norm not in indice:
            indice[sku_norm] = {
                "sku": sku_original,
                "nome": nome,
                "marca": marca,
                "_is_iaf": is_iaf
            }

        # Create variation without leading zero (for reverse match)
        if len(sku_norm) == 5 and sku_norm.startswith('0'):
            sku_sem_zero = sku_norm[1:]
            if sku_sem_zero not in indice:
                indice[sku_sem_zero] = {
                    "sku": sku_original,
                    "nome": nome,
                    "marca": marca,
                    "_variacao": "sem_zero",
                    "_is_iaf": is_iaf
                }

        # Create variation with leading zero (for reverse match)
        if len(sku_norm) == 4:
            sku_com_zero = '0' + sku_norm
            if sku_com_zero not in indice:
                indice[sku_com_zero] = {
                    "sku": sku_original,
                    "nome": nome,
                    "marca": marca,
                    "_variacao": "com_zero",
                    "_is_iaf": is_iaf
                }

    # 1. Load from produtos table (main products)
    cursor.execute("SELECT sku, sku_normalizado, nome, marca FROM produtos")
    for row in cursor.fetchall():
        sku_original, sku_norm, nome, marca = row
        adicionar_ao_indice(sku_original, sku_norm, nome, marca, is_iaf=False)

    # 2. Load from iaf_cabelos table (Siège/Eudora hair products)
    try:
        cursor.execute("SELECT sku, sku_normalizado, descricao, marca FROM iaf_cabelos")
        for row in cursor.fetchall():
            sku_original, sku_norm, nome, marca = row
            # Siège is part of Eudora brand
            if marca.upper() in ('SIAGE', 'SIÀGE', 'SIEGE'):
                marca = 'Eudora'
            adicionar_ao_indice(sku_original, sku_norm, nome, marca, is_iaf=True)
    except sqlite3.OperationalError:
        pass  # Table doesn't exist

    # 3. Load from iaf_make table (makeup products)
    try:
        cursor.execute("SELECT sku, sku_normalizado, descricao, marca FROM iaf_make")
        for row in cursor.fetchall():
            sku_original, sku_norm, nome, marca = row
            adicionar_ao_indice(sku_original, sku_norm, nome, marca, is_iaf=True)
    except sqlite3.OperationalError:
        pass  # Table doesn't exist

    return indice


def buscar_sku_no_indice(
    codigo_produto: str,
    indice: Dict[str, Dict]
) -> Tuple[Optional[str], Optional[str], str]:
    """
    Search for a SKU in the in-memory index.

    Faster than searching in SQLite for batch processing.

    Args:
        codigo_produto: Product code
        indice: Index created by criar_indice_sku_em_memoria()

    Returns:
        Tuple (marca, nome, motivo_match)
    """
    sku_norm = normalizar_sku(codigo_produto)

    if not sku_norm:
        return None, None, MOTIVO_NAO_ENCONTRADO

    # Search in index (already includes variations)
    if sku_norm in indice:
        info = indice[sku_norm]
        variacao = info.get("_variacao")

        if variacao == "sem_zero":
            motivo = MOTIVO_MATCH_SEM_ZERO
        elif variacao == "com_zero":
            motivo = MOTIVO_MATCH_COM_ZERO
        else:
            motivo = MOTIVO_MATCH_EXATO

        return info["marca"], info["nome"], motivo

    # Try variations manually if not found
    if len(sku_norm) == 4:
        sku_com_zero = '0' + sku_norm
        if sku_com_zero in indice:
            info = indice[sku_com_zero]
            return info["marca"], info["nome"], MOTIVO_MATCH_COM_ZERO

    if len(sku_norm) >= 5 and sku_norm.startswith('0'):
        sku_sem_zero = sku_norm.lstrip('0') or '0'
        if sku_sem_zero in indice:
            info = indice[sku_sem_zero]
            return info["marca"], info["nome"], MOTIVO_MATCH_SEM_ZERO

    return None, None, MOTIVO_NAO_ENCONTRADO


def listar_produtos(
    conn: sqlite3.Connection,
    marca: Optional[str] = None,
    busca: Optional[str] = None,
    limite: int = 100,
    offset: int = 0
) -> Tuple[list, int]:
    """
    List products with optional filters.

    Args:
        conn: SQLite connection
        marca: Filter by brand
        busca: Search term (name or SKU)
        limite: Maximum number of results
        offset: Pagination offset

    Returns:
        Tuple of (list of products, total count)
    """
    cursor = conn.cursor()

    # Build query
    where_clauses = []
    params = []

    if marca:
        where_clauses.append("marca = ?")
        params.append(marca)

    if busca:
        where_clauses.append("(nome LIKE ? OR sku LIKE ? OR sku_normalizado LIKE ?)")
        busca_pattern = f"%{busca}%"
        params.extend([busca_pattern, busca_pattern, busca_pattern])

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # Get total count
    cursor.execute(f"SELECT COUNT(*) FROM produtos WHERE {where_sql}", params)
    total = cursor.fetchone()[0]

    # Get products
    cursor.execute(f"""
        SELECT id, sku, sku_normalizado, nome, marca, created_at
        FROM produtos
        WHERE {where_sql}
        ORDER BY nome
        LIMIT ? OFFSET ?
    """, params + [limite, offset])

    produtos = [
        {
            "id": row[0],
            "sku": row[1],
            "sku_normalizado": row[2],
            "nome": row[3],
            "marca": row[4],
            "created_at": row[5]
        }
        for row in cursor.fetchall()
    ]

    return produtos, total


def listar_marcas(conn: sqlite3.Connection) -> list:
    """
    List all unique brands in the database.

    Args:
        conn: SQLite connection

    Returns:
        List of brand names with counts
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT marca, COUNT(*) as qtde
        FROM produtos
        GROUP BY marca
        ORDER BY qtde DESC
    """)

    return [{"marca": row[0], "qtde": row[1]} for row in cursor.fetchall()]
