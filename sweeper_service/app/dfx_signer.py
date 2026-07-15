from __future__ import annotations

import hmac
import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from eth_account import Account
from eth_account.messages import encode_defunct

from .derivation import EvmDeriver
from .signing import build_signature


DFX_MESSAGE_PREFIX = (
    "By_signing_this_message,_you_confirm_that_you_are_the_sole_owner_of_the_"
    "provided_Blockchain_address._Your_ID:_"
)
_MAX_BODY_BYTES = 8192
_MAX_TIMESTAMP_SKEW_SECONDS = 60
_NONCE_TTL_SECONDS = 180


class _NonceStore:
    def __init__(self):
        self._values: dict[str, float] = {}
        self._lock = threading.Lock()

    def consume(self, nonce: str, now: float) -> bool:
        with self._lock:
            expired = [
                key
                for key, expires_at in self._values.items()
                if expires_at <= now
            ]
            for key in expired:
                self._values.pop(key, None)
            if nonce in self._values:
                return False
            self._values[nonce] = now + _NONCE_TTL_SECONDS
            return True


_NONCES = _NonceStore()


def build_dfx_message(address: str) -> str:
    normalized = str(address or "").strip().lower()
    if not normalized.startswith("0x") or len(normalized) != 42:
        raise ValueError("Invalid EVM address")
    return f"{DFX_MESSAGE_PREFIX}{normalized}"


def sign_dfx_message(*, config, chain: str, derivation_index: int, address: str) -> dict:
    normalized_chain = str(chain or "").strip().lower()
    normalized_address = str(address or "").strip().lower()
    normalized_index = int(derivation_index)
    if normalized_index < 0:
        raise ValueError("derivation_index must be >= 0")

    deriver = EvmDeriver(
        mnemonic=config.mnemonic,
        passphrase=config.mnemonic_passphrase,
        account_index=config.account_index,
    )
    derived_address = deriver.derive_address(
        chain=normalized_chain,
        address_index=normalized_index,
    )
    private_key = deriver.derive_private_key(
        chain=normalized_chain,
        address_index=normalized_index,
    )
    if derived_address.lower() != normalized_address:
        raise ValueError("Requested address does not match the derived address")

    message = build_dfx_message(normalized_address)
    signable = encode_defunct(text=message)
    signed = Account.sign_message(signable, private_key=private_key)
    signature = signed.signature.hex()
    if not signature.startswith("0x"):
        signature = f"0x{signature}"

    recovered = Account.recover_message(signable, signature=signature)
    if recovered.lower() != normalized_address:
        raise ValueError("Generated DFX signature failed local verification")

    return {
        "address": normalized_address,
        "message": message,
        "signature": signature,
    }


class _SignerServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address, handler_class, *, config):
        super().__init__(server_address, handler_class)
        self.config = config


class _Handler(BaseHTTPRequestHandler):
    server_version = "MediaCMSSweeperSigner/1"

    def log_message(self, format, *args):
        logging.debug("dfx_signer " + format, *args)

    def _json(self, status: int, payload: dict):
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"ok": True})
            return
        self._json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path != "/v1/sign/dfx":
            self._json(404, {"error": "not_found"})
            return

        try:
            length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            self._json(400, {"error": "invalid_content_length"})
            return
        if length <= 0 or length > _MAX_BODY_BYTES:
            self._json(413, {"error": "invalid_body_size"})
            return

        body = self.rfile.read(length)
        service_name = str(self.headers.get("X-Ledger-Service") or "").strip()
        timestamp = str(self.headers.get("X-Ledger-Timestamp") or "").strip()
        nonce = str(self.headers.get("X-Ledger-Nonce") or "").strip()
        supplied_signature = str(
            self.headers.get("X-Ledger-Signature") or ""
        ).strip().lower()

        expected_service = (
            os.environ.get("DFX_SIGNER_EXPECTED_SERVICE", "mediacms-web").strip()
            or "mediacms-web"
        )
        if service_name != expected_service:
            self._json(403, {"error": "invalid_service"})
            return

        gateway_secret = os.environ.get(
            "MEDIACMS_INTERNAL_GATEWAY_SECRET",
            "",
        ).strip()
        gateway_header = (
            os.environ.get(
                "MEDIACMS_INTERNAL_GATEWAY_HEADER",
                "X-Ledger-Internal-Gateway",
            ).strip()
            or "X-Ledger-Internal-Gateway"
        )
        if gateway_secret:
            supplied_gateway = str(
                self.headers.get(gateway_header) or ""
            ).strip()
            if not hmac.compare_digest(supplied_gateway, gateway_secret):
                self._json(403, {"error": "invalid_gateway"})
                return

        try:
            timestamp_value = int(timestamp)
        except ValueError:
            self._json(403, {"error": "invalid_timestamp"})
            return
        now = int(time.time())
        if abs(now - timestamp_value) > _MAX_TIMESTAMP_SKEW_SECONDS:
            self._json(403, {"error": "expired_timestamp"})
            return
        if not nonce or len(nonce) > 128:
            self._json(403, {"error": "invalid_nonce"})
            return

        expected_signature = build_signature(
            service_name=service_name,
            timestamp=timestamp,
            nonce=nonce,
            body_bytes=body,
            shared_secret=self.server.config.shared_secret,
        )
        if not hmac.compare_digest(supplied_signature, expected_signature):
            self._json(403, {"error": "invalid_signature"})
            return
        if not _NONCES.consume(nonce, time.monotonic()):
            self._json(409, {"error": "replayed_nonce"})
            return

        try:
            payload = json.loads(body.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("JSON body must be an object")
            result = sign_dfx_message(
                config=self.server.config,
                chain=payload.get("chain"),
                derivation_index=payload.get("derivation_index"),
                address=payload.get("address"),
            )
        except Exception as exc:
            self._json(400, {"error": str(exc)[:500]})
            return

        self._json(200, result)


def start_dfx_signer_server(config):
    host = os.environ.get("DFX_SIGNER_HOST", "0.0.0.0").strip() or "0.0.0.0"
    port = int(os.environ.get("DFX_SIGNER_PORT", "8080"))
    if port <= 0 or port > 65535:
        raise RuntimeError("DFX_SIGNER_PORT must be between 1 and 65535")

    server = _SignerServer((host, port), _Handler, config=config)
    thread = threading.Thread(
        target=server.serve_forever,
        name="dfx-signer",
        daemon=True,
    )
    thread.start()
    logging.info("dfx_signer action=started host=%s port=%s", host, port)
    return server


__all__ = [
    "DFX_MESSAGE_PREFIX",
    "build_dfx_message",
    "sign_dfx_message",
    "start_dfx_signer_server",
]
