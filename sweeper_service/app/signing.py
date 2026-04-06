import hashlib
import hmac
import json
import time
import uuid


def build_signature(
    *,
    service_name: str,
    timestamp: str,
    nonce: str,
    body_bytes: bytes,
    shared_secret: str,
) -> str:
    body_sha256 = hashlib.sha256(body_bytes).hexdigest()
    signing_payload = "\n".join(
        [
            service_name,
            timestamp,
            nonce,
            body_sha256,
        ]
    ).encode("utf-8")

    return hmac.new(
        shared_secret.encode("utf-8"),
        signing_payload,
        hashlib.sha256,
    ).hexdigest()


def build_signed_json_request(*, service_name: str, shared_secret: str, payload: dict):
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    timestamp = str(int(time.time()))
    nonce = uuid.uuid4().hex
    signature = build_signature(
        service_name=service_name,
        timestamp=timestamp,
        nonce=nonce,
        body_bytes=body,
        shared_secret=shared_secret,
    )

    headers = {
        "Content-Type": "application/json",
        "X-Ledger-Service": service_name,
        "X-Ledger-Timestamp": timestamp,
        "X-Ledger-Nonce": nonce,
        "X-Ledger-Signature": signature,
    }
    return body, headers