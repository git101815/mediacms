from datetime import datetime, timedelta, timezone

import pytest

from deposit_service.app.runtime_price import (
    canonical_usd_to_required_pol_wei,
    fetch_pol_usd_quote,
    normalize_pol_usd_quote,
)


def _quote(price="0.25"):
    now = datetime.now(timezone.utc)
    return {
        "asset": "POL",
        "currency": "USD",
        "price": price,
        "source": "test",
        "observed_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=2)).isoformat(),
    }


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_fetch_pol_quote_uses_runtime_service_and_secret(monkeypatch):
    calls = []

    def fake_get(url, *, headers, timeout):
        calls.append((url, headers, timeout))
        return _Response(_quote())

    monkeypatch.setattr("deposit_service.app.runtime_price.httpx.get", fake_get)

    quote = fetch_pol_usd_quote(
        base_url="https://runtime.invalid/webhook/secret-path",
        shared_secret="shared-secret",
        timeout_seconds=5,
        max_age_seconds=180,
        future_skew_seconds=30,
    )

    assert calls == [
        (
            "https://runtime.invalid/webhook/secret-path/ledger/runtime-prices/pol-usd",
            {"X-Internal-Shared-Secret": "shared-secret"},
            5.0,
        )
    ]
    assert quote["price"] == "0.25"


def test_runtime_quote_is_used_for_native_threshold():
    quote = normalize_pol_usd_quote(
        _quote("0.25"),
        max_age_seconds=180,
        future_skew_seconds=30,
    )
    assert canonical_usd_to_required_pol_wei(10_000_000, quote) == 40 * 10**18


def test_stale_runtime_quote_is_rejected():
    now = datetime.now(timezone.utc)
    quote = _quote()
    quote["observed_at"] = (now - timedelta(minutes=4)).isoformat()
    quote["expires_at"] = (now + timedelta(minutes=2)).isoformat()
    with pytest.raises(RuntimeError):
        normalize_pol_usd_quote(
            quote,
            max_age_seconds=180,
            future_skew_seconds=30,
        )
