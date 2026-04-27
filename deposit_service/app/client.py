import os
import httpx

from .signing import build_signed_json_request


DEFAULT_INTERNAL_GATEWAY_HEADER = "X-Ledger-Internal-Gateway"

class MediaCMSInternalClient:
    def __init__(
        self,
        *,
        base_url: str,
        service_name: str,
        shared_secret: str,
        timeout: float = 10.0,
    ):
        self._base_url = base_url.rstrip("/")
        self._service_name = service_name
        self._shared_secret = shared_secret
        self._gateway_header = os.environ.get(
            "MEDIACMS_INTERNAL_GATEWAY_HEADER",
            DEFAULT_INTERNAL_GATEWAY_HEADER,
        ).strip() or DEFAULT_INTERNAL_GATEWAY_HEADER
        self._gateway_secret = (
            os.environ.get("MEDIACMS_INTERNAL_GATEWAY_SECRET", "").strip()
            or os.environ.get("LEDGER_INTERNAL_GATEWAY_SECRET", "").strip()
        )
        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def post_signed(self, path: str, payload: dict) -> dict:
        body, headers = build_signed_json_request(
            service_name=self._service_name,
            shared_secret=self._shared_secret,
            payload=payload,
        )
        if self._gateway_secret:
            headers[self._gateway_header] = self._gateway_secret

        response = self._client.post(f"{self._base_url}{path}", content=body, headers=headers)
        if response.status_code >= 400:
            raise RuntimeError(f"Internal API error {response.status_code} for {path}: {response.text}")
        return response.json()

    def get_watchlist(self, options: list[dict]) -> list[dict]:
        result = self.post_signed("/api/internal/ledger/deposit-watchlist", {"options": options})
        return result["results"]