from datetime import timedelta

import pytest
from django.core.exceptions import ValidationError
from django.utils import timezone

from ledger.paygate_polygon import (
    canonical_usd_to_required_pol_wei,
    get_paygate_polygon_credit_minimum_canonical,
    normalize_pol_usd_quote,
    pol_wei_to_canonical_usd,
)


def _quote(price="0.10"):
    now = timezone.now()
    return {
        "asset": "POL",
        "currency": "USD",
        "price": price,
        "source": "test",
        "observed_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=2)).isoformat(),
    }


def test_pol_quote_validation_and_usd_valuation():
    quote = normalize_pol_usd_quote(_quote("0.10"), require_current=True)
    assert pol_wei_to_canonical_usd(100 * 10**18, quote) == 10_000_000


def test_required_pol_wei_uses_runtime_price_payload():
    quote = normalize_pol_usd_quote(_quote("0.25"), require_current=True)
    required = canonical_usd_to_required_pol_wei(10_000_000, quote)
    assert required == 40 * 10**18


def test_expired_pol_quote_is_rejected():
    now = timezone.now()
    quote = _quote("0.10")
    quote["observed_at"] = (now - timedelta(minutes=4)).isoformat()
    quote["expires_at"] = (now - timedelta(minutes=1)).isoformat()
    with pytest.raises(ValidationError):
        normalize_pol_usd_quote(quote, require_current=True)


def test_paygate_polygon_credit_minimum_uses_versioned_tolerance():
    snapshot = {
        "token_amount": 1000 * 10**6,
        "net_stable_amount": 10_000_000,
        "gross_stable_amount": 10_500_000,
    }
    assert get_paygate_polygon_credit_minimum_canonical(snapshot) == 10_000_000
