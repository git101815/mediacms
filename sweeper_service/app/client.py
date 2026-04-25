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
        if response.status_code >= 400:
            raise RuntimeError(
                f"Internal API error {response.status_code} for {path}: {response.text}"
            )
        return response.json()

    def claim_jobs(self, *, options: list[dict], limit: int) -> list[dict]:
        result = self.post_signed(
            "/api/internal/ledger/sweep-jobs/claim",
            {
                "options": options,
                "limit": int(limit),
            },
        )
        return result["results"]

    def mark_funding_broadcasted(
        self,
        *,
        public_id: str,
        gas_funding_txid: str,
        destination_address: str,
        last_sweep_gas_limit: int | None = None,
    ) -> dict:
        payload = {
            "gas_funding_txid": gas_funding_txid,
            "destination_address": destination_address,
        }
        if last_sweep_gas_limit is not None:
            payload["last_sweep_gas_limit"] = int(last_sweep_gas_limit)

        return self.post_signed(
            f"/api/internal/ledger/sweep-jobs/{public_id}/funding-broadcasted",
            payload,
        )

    def mark_ready_to_sweep(self, *, public_id: str) -> dict:
        return self.post_signed(
            f"/api/internal/ledger/sweep-jobs/{public_id}/ready-to-sweep",
            {},
        )

    def mark_sweep_broadcasted(
        self,
        *,
        public_id: str,
        sweep_txid: str,
        destination_address: str,
        last_sweep_gas_limit: int | None = None,
    ) -> dict:
        payload = {
            "sweep_txid": sweep_txid,
            "destination_address": destination_address,
        }
        if last_sweep_gas_limit is not None:
            payload["last_sweep_gas_limit"] = int(last_sweep_gas_limit)

        return self.post_signed(
            f"/api/internal/ledger/sweep-jobs/{public_id}/sweep-broadcasted",
            payload,
        )

    def mark_confirmed(self, *, public_id: str) -> dict:
        return self.post_signed(
            f"/api/internal/ledger/sweep-jobs/{public_id}/confirmed",
            {},
        )

    def mark_failed(self, *, public_id: str, error: str) -> dict:
        return self.post_signed(
            f"/api/internal/ledger/sweep-jobs/{public_id}/failed",
            {
                "error": error,
            },
        )

    def mark_rescheduled(
        self,
        *,
        public_id: str,
        next_retry_in_seconds: int,
        error: str = "",
        error_code: str = "",
        retryable: bool = True,
        increment_retry_count: bool = False,
    ) -> dict:
        return self.post_signed(
            f"/api/internal/ledger/sweep-jobs/{public_id}/reschedule",
            {
                "next_retry_in_seconds": int(next_retry_in_seconds),
                "error": error,
                "error_code": error_code,
                "retryable": bool(retryable),
                "increment_retry_count": bool(increment_retry_count),
            },
        )
