"""
FastAPI dependencies for database and session management.
"""
import sqlite3
from typing import Generator

from app.config import DATABASE_PATH


def get_db() -> Generator[sqlite3.Connection, None, None]:
    """
    Dependency that provides a database connection.

    Yields:
        SQLite connection with Row factory enabled
    """
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()
