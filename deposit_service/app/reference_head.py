import datetime as dt
import logging

import httpx


def _parse_iso8601(value: str) -> dt.datetime:
    text = str(value or "").strip()
    if not text:
        raise RuntimeError("Missing updated_at in reference head payload")

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    parsed = dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise RuntimeError("reference updated_at must include timezone")
    return parsed


def get_reference_head(
    *,
    chain: str,
    base_url: str,
    shared_secret: str,
    timeout_seconds: float,
    max_age_seconds: int,
) -> int | None:
    url = f"{base_url.rstrip('/')}/ledger/reference-heads/{chain}"
    headers = {
        "X-Internal-Shared-Secret": shared_secret,
    }

    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            payload = response.json()

        latest_block = payload.get("latest_block")
        if latest_block is None:
            raise RuntimeError(
                f"Missing latest_block in reference payload for chain={chain}: {payload}"
            )

        updated_at = _parse_iso8601(payload.get("updated_at"))
        now = dt.datetime.now(dt.timezone.utc)
        age_seconds = int((now - updated_at).total_seconds())

        if age_seconds < 0:
            raise RuntimeError(
                f"Reference payload timestamp is in the future for chain={chain}: {payload}"
            )

        if age_seconds > int(max_age_seconds):
            raise RuntimeError(
                f"Reference payload is stale for chain={chain}: age_seconds={age_seconds}, max_age_seconds={max_age_seconds}"
            )

        return int(latest_block)
    except Exception as exc:
        logging.warning(
            "reference head unavailable chain=%s error=%s fallback=internal-rpc-health-checks",
            chain,
            exc,
        )
        return None