import httpx

from .signing import build_signed_json_request


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
        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def post_signed(self, path: str, payload: dict) -> dict:
        body, headers = build_signed_json_request(
            service_name=self._service_name,
            shared_secret=self._shared_secret,
            payload=payload,
        )
        response = self._client.post(f"{self._base_url}{path}", content=body, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_pool_stats(self, options: list[dict]) -> list[dict]:
        result = self.post_signed(
            "/api/internal/ledger/deposit-addresses/stats",
            {"options": options},
        )
        return result["results"]

    def provision_addresses(self, addresses: list[dict]) -> dict:
        return self.post_signed(
            "/api/internal/ledger/deposit-addresses/provision",
            {"addresses": addresses},
        )