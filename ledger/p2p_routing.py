from django.urls import re_path

from .p2p_consumers import P2PExchangeConsumer


UUID_RE = (
    r"[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}"
)

websocket_urlpatterns = [
    re_path(
        rf"^ws/p2p/(?P<public_id>{UUID_RE})/$",
        P2PExchangeConsumer.as_asgi(),
    ),
]
