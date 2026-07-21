import base64
from types import SimpleNamespace

from sweeper_service.app.dfx_signer import (
    build_mtpelerin_message,
    sign_mtpelerin_message,
)


def test_mtpelerin_official_evm_test_vector():
    config = SimpleNamespace(
        mnemonic="bamboo feed assist glove soda merry medal vanish almost solid bean loop",
        mnemonic_passphrase="",
        account_index=0,
    )
    result = sign_mtpelerin_message(
        config=config,
        chain="ethereum",
        derivation_index=0,
        address="0xEa22e16EA50A43092853329F3cEEa0825Cb9B03e",
        code="1234",
    )

    assert result["message"] == "MtPelerin-1234"
    assert result["address"] == "0xea22e16ea50a43092853329f3ceea0825cb9b03e"
    assert result["signature"] == (
        "yrXNJSmMc4wvVyKEzN4cEmLTvEaridjqTULZAfMwYAMM5PgBz4fCoIWNLr5NwKhxOYiPpI2vhMlKCihWadUw5xs="
    )
    assert len(base64.b64decode(result["signature"], validate=True)) == 65


def test_mtpelerin_message_rejects_invalid_code():
    for value in ("", "999", "10000", "abcd"):
        try:
            build_mtpelerin_message(value)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid code accepted: {value!r}")
