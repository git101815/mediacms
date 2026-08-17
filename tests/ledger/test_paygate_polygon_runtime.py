from datetime import timedelta
from decimal import Decimal

import pytest
from django.core.cache import cache
from django.utils import timezone

from ledger.paygate_polygon import (
    PAYGATE_POLYGON_QUOTE_CACHE_KEY,
    canonical_usd_to_required_pol_wei,
    get_fresh_pol_usd_quote,
    get_paygate_polygon_credit_minimum_canonical,
    pol_wei_to_canonical_usd,
    store_pol_usd_quote,
)


@pytest.fixture(autouse=True)
def clear_pol_quote_cache():
    cache.delete(PAYGATE_POLYGON_QUOTE_CACHE_KEY)
    yield
    cache.delete(PAYGATE_POLYGON_QUOTE_CACHE_KEY)


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


def test_pol_quote_round_trip_and_usd_valuation():
    stored = store_pol_usd_quote(_quote("0.10"))
    assert get_fresh_pol_usd_quote(required=True) == stored
    assert pol_wei_to_canonical_usd(100 * 10**18, stored) == 10_000_000


def test_required_pol_wei_uses_runtime_price():
    stored = store_pol_usd_quote(_quote("0.25"))
    required = canonical_usd_to_required_pol_wei(10_000_000, stored)
    assert required == 40 * 10**18


def test_paygate_polygon_credit_minimum_uses_versioned_tolerance():
    snapshot = {
        "token_amount": 1000 * 10**6,
        "net_stable_amount": 10_000_000,
        "gross_stable_amount": 10_500_000,
    }
    # 5% drift is allowed against gross, but never below immutable token net value.
    assert get_paygate_polygon_credit_minimum_canonical(snapshot) == 10_000_000
