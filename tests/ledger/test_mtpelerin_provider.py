from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest

from ledger.providers import mtpelerin


def test_public_direct_link_prefills_amount_without_personal_data(settings):
    settings.MTPERELIN_DIRECT_LINK_CTKN = "public-direct-link-key"
    settings.MTPERELIN_WIDGET_BASE_URL = "https://widget.mtpelerin.com"
    settings.MTPERELIN_LANGUAGE = "en"

    url = mtpelerin.build_mtpelerin_checkout_url(
        fiat_currency="EUR",
        chain="base",
        asset_code="USDC",
        source_amount="22.10",
        target_canonical_amount=25_000_000,
        address="0x1111111111111111111111111111111111111111",
        validation_code="1234",
        validation_signature_b64="abc+/=",
    )

    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    assert parsed.scheme == "https"
    assert parsed.netloc == "widget.mtpelerin.com"
    assert query["_ctkn"] == ["public-direct-link-key"]
    assert query["type"] == ["direct-link"]
    assert query["tabs"] == ["buy"]
    assert query["tab"] == ["buy"]
    assert query["bsc"] == ["EUR"]
    assert query["bdc"] == ["USDC"]
    assert query["bsa"] == ["22.10"]
    assert query["bda"] == ["25"]
    assert query["curs"] == ["EUR"]
    assert query["crys"] == ["USDC"]
    assert query["net"] == ["base_mainnet"]
    assert query["nets"] == ["base_mainnet"]
    assert query["dnet"] == ["base_mainnet"]
    assert query["addr"] == ["0x1111111111111111111111111111111111111111"]
    assert query["code"] == ["1234"]
    assert query["hash"] == ["abc+/="]
    for personal_field in (
        "email", "phone", "firstname", "lastname",
        "firstName", "lastName", "username", "user_id",
    ):
        assert personal_field not in query


def test_quote_uses_destination_amount_and_bank_transfer(monkeypatch, settings):
    settings.MTPERELIN_FIAT_CURRENCIES = ("EUR", "USD")
    monkeypatch.setattr(
        mtpelerin,
        "mtpelerin_route_available",
        lambda **kwargs: True,
    )
    calls = []

    def fake_http_json(*, method, path, payload=None):
        calls.append((method, path, payload))
        return {
            "sourceCurrency": "EUR",
            "sourceNetwork": "fiat",
            "sourceAmount": "22.10",
            "destCurrency": "USDC",
            "destNetwork": "base_mainnet",
            "destAmount": "25",
            "fees": {"networkFee": "0", "fixFee": 0},
        }

    monkeypatch.setattr(mtpelerin, "_http_json", fake_http_json)
    quote = mtpelerin.get_mtpelerin_quote(
        fiat_currency="EUR",
        chain="base",
        asset_code="USDC",
        target_canonical_amount=25_000_000,
        force_refresh=True,
    )

    assert Decimal(quote["sourceAmount"]) == Decimal("22.10")
    assert quote["requestedTargetAmount"] == "25"
    assert calls == [
        (
            "POST",
            "currency_rates/convert",
            {
                "sourceCurrency": "EUR",
                "sourceNetwork": "fiat",
                "destAmount": 25.0,
                "destCurrency": "USDC",
                "destNetwork": "base_mainnet",
                "isCardPayment": False,
            },
        )
    ]


@pytest.mark.parametrize(
    ("chain", "network"),
    [
        ("base", "base_mainnet"),
        ("bsc", "bsc_mainnet"),
        ("arbitrum", "arbitrum_mainnet"),
        ("ethereum", "mainnet"),
    ],
)
def test_chain_mapping(chain, network):
    assert mtpelerin.get_mtpelerin_network(chain) == network
