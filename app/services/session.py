"""
Session management service.

Manages per-user sessions with UUID-based session IDs.
Sessions are stored in memory with automatic expiration.
"""
import uuid
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, Optional

import polars as pl


# Session configuration
SESSION_EXPIRATION_HOURS = 24  # Sessions expire after 24 hours
MAX_SESSIONS = 100  # Maximum concurrent sessions (to prevent memory issues)

# Thread-safe session storage
_sessions: Dict[str, Dict[str, Any]] = {}
_sessions_lock = Lock()


def _create_empty_session() -> Dict[str, Any]:
    """Create a new empty session data structure."""
    return {
        "df_vendas": None,
        "df_clientes": None,
        "df_iaf": None,
        "created_at": datetime.utcnow(),
        "last_access": datetime.utcnow(),
    }


def generate_session_id() -> str:
    """Generate a new unique session ID."""
    return str(uuid.uuid4())


def get_session(session_id: Optional[str]) -> tuple[str, Dict[str, Any]]:
    """
    Get or create a session.

    Args:
        session_id: Existing session ID or None for new session

    Returns:
        Tuple of (session_id, session_data)
    """
    with _sessions_lock:
        # Clean expired sessions periodically
        _cleanup_expired_sessions()

        # If session_id provided and exists, return it
        if session_id and session_id in _sessions:
            _sessions[session_id]["last_access"] = datetime.utcnow()
            return session_id, _sessions[session_id]

        # Create new session
        new_id = generate_session_id()
        _sessions[new_id] = _create_empty_session()

        return new_id, _sessions[new_id]


def get_session_data(session_id: str) -> Optional[Dict[str, Any]]:
    """
    Get session data by ID.

    Args:
        session_id: Session ID

    Returns:
        Session data or None if not found
    """
    with _sessions_lock:
        if session_id in _sessions:
            _sessions[session_id]["last_access"] = datetime.utcnow()
            return _sessions[session_id]
        return None


def set_session_value(session_id: str, key: str, value: Any) -> bool:
    """
    Set a value in a session.

    Args:
        session_id: Session ID
        key: Key to set
        value: Value to store

    Returns:
        True if successful, False if session not found
    """
    with _sessions_lock:
        if session_id not in _sessions:
            return False

        _sessions[session_id][key] = value
        _sessions[session_id]["last_access"] = datetime.utcnow()
        return True


def clear_session(session_id: str) -> bool:
    """
    Clear session data (but keep the session).

    Args:
        session_id: Session ID

    Returns:
        True if successful, False if session not found
    """
    with _sessions_lock:
        if session_id not in _sessions:
            return False

        _sessions[session_id]["df_vendas"] = None
        _sessions[session_id]["df_clientes"] = None
        _sessions[session_id]["df_iaf"] = None
        _sessions[session_id]["last_access"] = datetime.utcnow()
        return True


def delete_session(session_id: str) -> bool:
    """
    Delete a session completely.

    Args:
        session_id: Session ID

    Returns:
        True if deleted, False if not found
    """
    with _sessions_lock:
        if session_id in _sessions:
            del _sessions[session_id]
            return True
        return False


def _cleanup_expired_sessions() -> int:
    """
    Remove expired sessions.

    Returns:
        Number of sessions removed
    """
    # Must be called with lock held
    now = datetime.utcnow()
    expiration_threshold = now - timedelta(hours=SESSION_EXPIRATION_HOURS)

    expired_ids = [
        sid for sid, data in _sessions.items()
        if data.get("last_access", data.get("created_at", now)) < expiration_threshold
    ]

    for sid in expired_ids:
        del _sessions[sid]

    # Also remove oldest sessions if we're over the limit
    if len(_sessions) > MAX_SESSIONS:
        # Sort by last_access and remove oldest
        sorted_sessions = sorted(
            _sessions.items(),
            key=lambda x: x[1].get("last_access", datetime.min)
        )

        to_remove = len(_sessions) - MAX_SESSIONS
        for sid, _ in sorted_sessions[:to_remove]:
            del _sessions[sid]
            expired_ids.append(sid)

    return len(expired_ids)


def get_session_stats() -> Dict[str, Any]:
    """
    Get session statistics for monitoring.

    Returns:
        Dict with session stats
    """
    with _sessions_lock:
        total = len(_sessions)

        if total == 0:
            return {
                "total_sessions": 0,
                "active_sessions": 0,
                "sessions_with_data": 0,
            }

        active_threshold = datetime.utcnow() - timedelta(hours=1)
        active = sum(
            1 for data in _sessions.values()
            if data.get("last_access", datetime.min) > active_threshold
        )

        with_data = sum(
            1 for data in _sessions.values()
            if data.get("df_vendas") is not None
        )

        return {
            "total_sessions": total,
            "active_sessions": active,
            "sessions_with_data": with_data,
        }
