from __future__ import annotations

import logging

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer


logger = logging.getLogger(__name__)


def p2p_group_name(public_id) -> str:
    return f"p2p.{str(public_id).replace('-', '').lower()}"


def publish_p2p_order_status(*, public_id, status: str, can_send: bool) -> None:
    """Best-effort realtime status broadcast. PostgreSQL remains authoritative."""
    try:
        channel_layer = get_channel_layer()
        if channel_layer is None:
            return
        async_to_sync(channel_layer.group_send)(
            p2p_group_name(public_id),
            {
                "type": "p2p.status",
                "order_status": str(status),
                "can_send": bool(can_send),
            },
        )
    except Exception:
        # A Redis/realtime outage must never make an order save fail. Clients
        # recover the authoritative status from PostgreSQL on reconnect.
        logger.exception(
            "Failed to broadcast P2P order status for %s",
            public_id,
        )
