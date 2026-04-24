"""
Slack integration service.

Sends sector goal cards as images to supervisoras via Slack DM.
"""
import base64
import io
from typing import Optional

from app.config import SLACK_BOT_TOKEN, SLACK_USER_MAP, SLACK_DEFAULT_USER_ID


def _get_client():
    if not SLACK_BOT_TOKEN:
        raise ValueError("SLACK_BOT_TOKEN não configurado")
    from slack_sdk import WebClient
    return WebClient(token=SLACK_BOT_TOKEN)


def resolver_slack_id(supervisora: str) -> str:
    """Return Slack user ID for a supervisora name, falling back to default."""
    if not supervisora:
        return SLACK_DEFAULT_USER_ID
    return SLACK_USER_MAP.get(supervisora.upper().strip(), SLACK_DEFAULT_USER_ID)


def enviar_meta_slack(
    supervisora: str,
    setor: str,
    image_base64: str,
) -> dict:
    """
    Send a sector goal card image to a supervisora via Slack DM.

    Args:
        supervisora: Supervisora name (used to look up Slack ID)
        setor: Sector name (used in message text)
        image_base64: Base64-encoded PNG image (data URL or raw base64)

    Returns:
        {"ok": True} on success or {"ok": False, "error": str} on failure
    """
    try:
        client = _get_client()
        user_id = resolver_slack_id(supervisora)

        # Strip data URL prefix if present
        if "," in image_base64:
            image_base64 = image_base64.split(",", 1)[1]

        image_bytes = base64.b64decode(image_base64)
        filename = f"meta_{setor.replace(' ', '_')[:40]}.png"

        # Open DM channel
        dm = client.conversations_open(users=[user_id])
        channel_id = dm["channel"]["id"]

        # Upload image directly to DM
        client.files_upload_v2(
            channel=channel_id,
            file=io.BytesIO(image_bytes),
            filename=filename,
            title=f"Meta — {setor}",
            initial_comment=f"📊 *Resultado de Metas — {setor}*\nAqui está o seu card com os resultados mais recentes!",
        )

        return {"ok": True}

    except Exception as exc:
        return {"ok": False, "error": str(exc)}
