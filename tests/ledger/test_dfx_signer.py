from types import SimpleNamespace

from django.test import SimpleTestCase
from eth_account import Account
from eth_account.messages import encode_defunct

from sweeper_service.app.derivation import EvmDeriver
from sweeper_service.app.dfx_signer import sign_dfx_message


class TestDfxSigner(SimpleTestCase):
    mnemonic = (
        "abandon abandon abandon abandon abandon abandon abandon "
        "abandon abandon abandon abandon about"
    )

    def _config(self):
        return SimpleNamespace(
            mnemonic=self.mnemonic,
            mnemonic_passphrase="",
            account_index=0,
        )

    def test_signer_returns_recoverable_signature_for_exact_derived_address(self):
        deriver = EvmDeriver(
            mnemonic=self.mnemonic,
            passphrase="",
            account_index=0,
        )
        address = deriver.derive_address(
            chain="arbitrum",
            address_index=7,
        )

        result = sign_dfx_message(
            config=self._config(),
            chain="arbitrum",
            derivation_index=7,
            address=address,
        )
        recovered = Account.recover_message(
            encode_defunct(text=result["message"]),
            signature=result["signature"],
        )
        self.assertEqual(recovered.lower(), address.lower())
        self.assertTrue(result["message"].endswith(address.lower()))

    def test_signer_rejects_address_not_owned_by_derivation_index(self):
        with self.assertRaisesRegex(
            ValueError,
            "does not match the derived address",
        ):
            sign_dfx_message(
                config=self._config(),
                chain="arbitrum",
                derivation_index=7,
                address="0x1111111111111111111111111111111111111111",
            )


import json as _json
import os as _os
import threading as _threading
import time as _time
import uuid as _uuid
from urllib.error import HTTPError as _HTTPError
from urllib.request import Request as _Request, urlopen as _urlopen

from sweeper_service.app.dfx_signer import _Handler as _DfxHandler
from sweeper_service.app.dfx_signer import _SignerServer as _DfxSignerServer
from sweeper_service.app.signing import build_signature as _build_signature


class TestDfxSignerHttpSecurity(SimpleTestCase):
    mnemonic = (
        "abandon abandon abandon abandon abandon abandon abandon "
        "abandon abandon abandon abandon about"
    )
    shared_secret = "test-dfx-shared-secret"
    service_name = "mediacms-web"

    def setUp(self):
        self.environment = patch.dict(
            _os.environ,
            {
                "DFX_SIGNER_EXPECTED_SERVICE": self.service_name,
                "MEDIACMS_INTERNAL_GATEWAY_SECRET": "",
            },
            clear=False,
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)

        self.config = SimpleNamespace(
            mnemonic=self.mnemonic,
            mnemonic_passphrase="",
            account_index=0,
            shared_secret=self.shared_secret,
        )
        self.server = _DfxSignerServer(
            ("127.0.0.1", 0),
            _DfxHandler,
            config=self.config,
        )
        self.thread = _threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.addCleanup(self._stop_server)

        deriver = EvmDeriver(
            mnemonic=self.mnemonic,
            passphrase="",
            account_index=0,
        )
        self.address = deriver.derive_address(
            chain="arbitrum",
            address_index=7,
        ).lower()

    def _stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def _request(
        self,
        *,
        timestamp=None,
        nonce=None,
        signature=None,
    ):
        payload = {
            "chain": "arbitrum",
            "derivation_index": 7,
            "address": self.address,
        }
        body = _json.dumps(
            payload,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        timestamp = str(timestamp or int(_time.time()))
        nonce = nonce or _uuid.uuid4().hex
        if signature is None:
            signature = _build_signature(
                service_name=self.service_name,
                timestamp=timestamp,
                nonce=nonce,
                body_bytes=body,
                shared_secret=self.shared_secret,
            )

        request = _Request(
            (
                "http://127.0.0.1:"
                f"{self.server.server_address[1]}/v1/sign/dfx"
            ),
            data=body,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Ledger-Service": self.service_name,
                "X-Ledger-Timestamp": timestamp,
                "X-Ledger-Nonce": nonce,
                "X-Ledger-Signature": signature,
            },
        )
        try:
            with _urlopen(request, timeout=3) as response:
                return response.status, _json.loads(response.read())
        except _HTTPError as exc:
            return exc.code, _json.loads(exc.read())

    def test_http_signer_rejects_expired_timestamp(self):
        status, payload = self._request(
            timestamp=int(_time.time()) - 120,
        )
        self.assertEqual(status, 403)
        self.assertEqual(payload["error"], "expired_timestamp")

    def test_http_signer_rejects_invalid_hmac(self):
        status, payload = self._request(signature="0" * 64)
        self.assertEqual(status, 403)
        self.assertEqual(payload["error"], "invalid_signature")

    def test_http_signer_rejects_replayed_nonce(self):
        timestamp = int(_time.time())
        nonce = _uuid.uuid4().hex

        first_status, first_payload = self._request(
            timestamp=timestamp,
            nonce=nonce,
        )
        second_status, second_payload = self._request(
            timestamp=timestamp,
            nonce=nonce,
        )

        self.assertEqual(first_status, 200)
        self.assertIn("signature", first_payload)
        self.assertEqual(second_status, 409)
        self.assertEqual(second_payload["error"], "replayed_nonce")

