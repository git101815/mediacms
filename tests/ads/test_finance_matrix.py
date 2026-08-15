import uuid
from types import SimpleNamespace

import pytest
from django.http import HttpResponse
from django.test import RequestFactory, override_settings

from ads import views


class FakeWalletViews:
    @staticmethod
    def _build_wallet_deposit_options():
        return [{"key": "route"}]

    @staticmethod
    def _build_wallet_token_pack_rows():
        return [{"code": "pack", "price_display": "5"}]

    @staticmethod
    def _build_recent_deposit_session_rows(wallet):
        return []

    @staticmethod
    def _build_deposit_session_payload(session):
        return {
            "status": "awaiting_payment",
            "status_label": "Waiting for payment",
            "display_label": "Crypto",
            "payment_method_label": "Crypto",
            "expected_payment_amount_display": "5",
            "expected_payment_currency": "USD",
            "network_display": "",
            "deposit_address": "",
            "observed_txid": "",
            "checkout_url": "",
            "is_terminal": False,
            "is_provider_checkout": False,
            "token_pack_name": "Starter tokens",
            "token_pack_label": "Starter · 500 tokens · $5",
        }

    @staticmethod
    def wallet_deposit_request(request):
        return HttpResponse("deposit-request")

    @staticmethod
    def wallet_deposit_session_cancel(request, public_id):
        return HttpResponse(f"cancel:{public_id}")

    @staticmethod
    def wallet_dfx_launch(request, public_id):
        return HttpResponse(f"dfx:{public_id}")

    @staticmethod
    def wallet_dfx_return(request, public_id):
        return HttpResponse(f"dfx-return:{public_id}")

    @staticmethod
    def wallet_mtpelerin_launch(request, public_id):
        return HttpResponse(f"mtpelerin:{public_id}")

    @staticmethod
    def wallet_banxa_launch(request, public_id):
        return HttpResponse(f"banxa:{public_id}")


def _fake_session():
    return SimpleNamespace(
        public_id=uuid.uuid4(),
        metadata={
            "token_pack": {
                "gross_stable_amount": 5_000_000,
                "name": "Starter",
                "token_amount": 500_000_000,
            }
        },
    )


def test_ads_pack_formatter_is_usd_only():
    assert views._format_ads_pack_usd(
        {
            "token_pack": {
                "gross_stable_amount": 5_000_000,
            }
        }
    ) == "$5"
    assert views._format_ads_pack_usd({}) == ""
    assert views._format_ads_pack_usd(
        {"token_pack": {"gross_stable_amount": "bad"}}
    ) == ""


def test_ads_deposit_payload_strips_token_pack_fields():
    session = _fake_session()
    payload = views._build_ads_deposit_session_payload(
        session,
        FakeWalletViews,
    )
    assert payload["ads_pack_usd"] == "$5"
    assert "token_pack_name" not in payload
    assert "token_pack_label" not in payload
    assert "tokens" not in str(payload).lower()


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_finance_page_uses_existing_wallet_backend_without_exposing_token_amount(
    client,
    advertiser_factory,
    monkeypatch,
):
    user = advertiser_factory()
    client.force_login(user)
    monkeypatch.setattr(
        views,
        "_wallet_views",
        lambda: FakeWalletViews,
    )
    monkeypatch.setattr(
        views,
        "_build_ads_recent_deposit_rows",
        lambda wallet, wallet_views: [],
    )

    response = client.get("/finance/")
    assert response.status_code == 200
    body = response.content.decode()
    assert "$" in body
    assert "tokens available" not in body.lower()


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_deposit_session_page_and_status_use_ads_safe_payload(
    client,
    advertiser_factory,
    monkeypatch,
):
    user = advertiser_factory()
    client.force_login(user)
    session = _fake_session()

    monkeypatch.setattr(
        views,
        "_wallet_views",
        lambda: FakeWalletViews,
    )
    monkeypatch.setattr(
        views,
        "get_object_or_404",
        lambda *args, **kwargs: session,
    )

    page = client.get(
        f"/finance/deposits/{session.public_id}/"
    )
    assert page.status_code == 200
    body = page.content.decode()
    assert "$5" in body
    assert "500 tokens" not in body

    status = client.get(
        f"/finance/deposits/{session.public_id}/status/"
    )
    assert status.status_code == 200
    data = status.json()
    assert data["ads_pack_usd"] == "$5"
    assert "token_pack_name" not in data
    assert "token_pack_label" not in data
    assert "tokens" not in str(data).lower()


@pytest.mark.django_db
def test_finance_deposit_request_delegates_to_existing_wallet_flow(
    monkeypatch,
):
    monkeypatch.setattr(
        views,
        "_wallet_views",
        lambda: FakeWalletViews,
    )
    request = RequestFactory().post("/finance/deposit-request/")
    response = views.finance_deposit_request(request)
    assert response.content == b"deposit-request"


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("view_name", "expected_prefix"),
    [
        ("finance_deposit_session_cancel", "cancel:"),
        ("finance_dfx_launch", "dfx:"),
        ("finance_dfx_return", "dfx-return:"),
        ("finance_mtpelerin_launch", "mtpelerin:"),
        ("finance_banxa_launch", "banxa:"),
    ],
)
def test_finance_provider_wrappers_delegate_without_reimplementing_provider_logic(
    monkeypatch,
    view_name,
    expected_prefix,
):
    monkeypatch.setattr(
        views,
        "_wallet_views",
        lambda: FakeWalletViews,
    )
    public_id = uuid.uuid4()
    factory = RequestFactory()
    request = (
        factory.post("/")
        if view_name
        in {
            "finance_deposit_session_cancel",
        }
        else factory.get("/")
    )
    response = getattr(views, view_name)(
        request,
        public_id,
    )
    assert response.content.decode().startswith(expected_prefix)
