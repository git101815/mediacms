import json
import time
import uuid

import pytest

from ledger.internal_api import build_internal_request_signature
from ledger.models import InternalAPIRequestNonce


pytestmark = pytest.mark.django_db

INTERNAL_WATCHLIST_URL = "/api/internal/ledger/deposit-watchlist"
INTERNAL_OBSERVATION_URL = "/api/internal/ledger/deposit-observations"


@pytest.fixture
def internal_api_config(settings, django_user_model):
    settings.LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME = "deposit_service_tests"
    settings.LEDGER_INTERNAL_DEPOSIT_SERVICE_SHARED_SECRET = "deposit-shared-secret-tests"
    settings.LEDGER_INTERNAL_SWEEPER_SERVICE_USERNAME = "sweeper_service_tests"
    settings.LEDGER_INTERNAL_SWEEPER_SERVICE_SHARED_SECRET = "sweeper-shared-secret-tests"
    settings.LEDGER_INTERNAL_API_MAX_SKEW_SECONDS = 300
    settings.LEDGER_INTERNAL_NONCE_TTL_SECONDS = 300
    settings.LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED = False
    settings.LEDGER_INTERNAL_GATEWAY_SECRET_REQUIRED = True
    settings.LEDGER_INTERNAL_GATEWAY_SECRET = "gateway-secret-tests"
    settings.LEDGER_INTERNAL_GATEWAY_HEADER = "X-Ledger-Internal-Gateway"
    settings.LEDGER_INTERNAL_API_ALLOWED_CIDRS = ["127.0.0.1/32"]

    django_user_model.objects.get_or_create(username="deposit_service_tests")
    django_user_model.objects.get_or_create(username="sweeper_service_tests")

    return {
        "deposit_secret": settings.LEDGER_INTERNAL_DEPOSIT_SERVICE_SHARED_SECRET,
        "gateway_secret": settings.LEDGER_INTERNAL_GATEWAY_SECRET,
    }


def _body(payload):
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _signed_headers(
    body_bytes,
    *,
    secret,
    service_name="deposit-service",
    timestamp=None,
    nonce=None,
    signature=None,
    gateway_secret=None,
):
    timestamp = str(int(time.time()) if timestamp is None else timestamp)
    nonce = nonce or uuid.uuid4().hex
    signature = signature or build_internal_request_signature(
        service_name=service_name,
        timestamp=timestamp,
        nonce=nonce,
        body_bytes=body_bytes,
        shared_secret=secret,
    )
    headers = {
        "HTTP_X_LEDGER_SERVICE": service_name,
        "HTTP_X_LEDGER_TIMESTAMP": timestamp,
        "HTTP_X_LEDGER_NONCE": nonce,
        "HTTP_X_LEDGER_SIGNATURE": signature,
    }
    if gateway_secret is not None:
        headers["HTTP_X_LEDGER_INTERNAL_GATEWAY"] = gateway_secret
    return headers


def _post_signed(
    client,
    url,
    payload,
    *,
    config,
    service_name="deposit-service",
    nonce=None,
    timestamp=None,
    signature=None,
    remote_addr="127.0.0.1",
    gateway_secret=None,
):
    body = _body(payload)
    return client.post(
        url,
        data=body,
        content_type="application/json",
        REMOTE_ADDR=remote_addr,
        **_signed_headers(
            body,
            secret=config["deposit_secret"],
            service_name=service_name,
            nonce=nonce,
            timestamp=timestamp,
            signature=signature,
            gateway_secret=gateway_secret,
        ),
    )


def test_internal_ledger_request_without_auth_headers_is_rejected(
    client,
    internal_api_config,
):
    before = InternalAPIRequestNonce.objects.count()

    response = client.post(
        INTERNAL_WATCHLIST_URL,
        data=b'{"options":[]}',
        content_type="application/json",
    )

    assert response.status_code == 403
    assert InternalAPIRequestNonce.objects.count() == before


def test_internal_ledger_rejects_wrong_service_name_without_creating_nonce(
    client,
    internal_api_config,
):
    before = InternalAPIRequestNonce.objects.count()

    response = _post_signed(
        client,
        INTERNAL_WATCHLIST_URL,
        {"options": []},
        config=internal_api_config,
        service_name="unexpected-service",
    )

    assert response.status_code == 403
    assert InternalAPIRequestNonce.objects.count() == before


def test_internal_ledger_rejects_bad_signature_without_creating_nonce(
    client,
    internal_api_config,
):
    before = InternalAPIRequestNonce.objects.count()

    response = _post_signed(
        client,
        INTERNAL_WATCHLIST_URL,
        {"options": []},
        config=internal_api_config,
        signature="0" * 64,
    )

    assert response.status_code == 403
    assert InternalAPIRequestNonce.objects.count() == before


def test_internal_ledger_rejects_timestamp_outside_allowed_skew(
    client,
    internal_api_config,
):
    stale_timestamp = int(time.time()) - 3600
    before = InternalAPIRequestNonce.objects.count()

    response = _post_signed(
        client,
        INTERNAL_WATCHLIST_URL,
        {"options": []},
        config=internal_api_config,
        timestamp=stale_timestamp,
    )

    assert response.status_code == 403
    assert InternalAPIRequestNonce.objects.count() == before


def test_internal_ledger_rejects_nonce_replay(client, internal_api_config):
    nonce = uuid.uuid4().hex

    first = _post_signed(
        client,
        INTERNAL_WATCHLIST_URL,
        {"options": []},
        config=internal_api_config,
        nonce=nonce,
    )
    second = _post_signed(
        client,
        INTERNAL_WATCHLIST_URL,
        {"options": []},
        config=internal_api_config,
        nonce=nonce,
    )

    assert first.status_code == 200
    assert first.json() == {"results": []}
    assert second.status_code == 403
    assert "Replay" in second.json()["error"]


def test_internal_ledger_rejects_signed_invalid_json_body(
    client,
    internal_api_config,
):
    body = b"{not-json"
    response = client.post(
        INTERNAL_WATCHLIST_URL,
        data=body,
        content_type="application/json",
        **_signed_headers(
            body,
            secret=internal_api_config["deposit_secret"],
        ),
    )

    assert response.status_code == 400
    assert "valid JSON" in response.json()["error"]


def test_internal_ledger_rejects_boolean_integer_fields_before_ingest(
    client,
    internal_api_config,
):
    response = _post_signed(
        client,
        INTERNAL_OBSERVATION_URL,
        {
            "session_public_id": str(uuid.uuid4()),
            "chain": "polygon",
            "txid": "0xdeadbeef",
            "log_index": None,
            "block_number": 1,
            "detected_block_number": 1,
            "from_address": "0xfrom",
            "deposit_address": "0xto",
            "token_contract_address": "0xtoken",
            "asset_code": "USDC",
            "amount": True,
            "confirmations": 1,
            "detection_method": "event",
        },
        config=internal_api_config,
    )

    assert response.status_code == 400
    assert "Invalid integer field: amount" in response.json()["error"]


def test_internal_ledger_network_guard_blocks_untrusted_remote_address(
    client,
    settings,
    internal_api_config,
):
    settings.LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED = True

    response = _post_signed(
        client,
        INTERNAL_WATCHLIST_URL,
        {"options": []},
        config=internal_api_config,
        remote_addr="203.0.113.10",
        gateway_secret=internal_api_config["gateway_secret"],
    )

    assert response.status_code == 403
    assert "source address" in response.json()["error"]


def test_internal_ledger_network_guard_requires_gateway_secret(
    client,
    settings,
    internal_api_config,
):
    settings.LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED = True

    response = _post_signed(
        client,
        INTERNAL_WATCHLIST_URL,
        {"options": []},
        config=internal_api_config,
        remote_addr="127.0.0.1",
        gateway_secret="wrong-gateway-secret",
    )

    assert response.status_code == 403
    assert "gateway" in response.json()["error"].lower()

