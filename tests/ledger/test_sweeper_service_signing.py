from sweeper_service.app.signing import build_signature


def test_build_signature_is_deterministic():
    body = b'{"a":1}'
    signature_1 = build_signature(
        service_name="sweeper-service",
        timestamp="1712400000",
        nonce="abc123",
        body_bytes=body,
        shared_secret="secret",
    )
    signature_2 = build_signature(
        service_name="sweeper-service",
        timestamp="1712400000",
        nonce="abc123",
        body_bytes=body,
        shared_secret="secret",
    )

    assert signature_1 == signature_2