from __future__ import annotations

import logging
import os
import time
from types import SimpleNamespace

from .dfx_signer import start_dfx_signer_server


def _read_secret(path: str, *, allow_empty: bool = False) -> str:
    normalized_path = str(path or "").strip()
    if not normalized_path:
        raise RuntimeError("DFX signer secret path must not be empty")

    try:
        with open(normalized_path, "r", encoding="utf-8") as handle:
            value = handle.read().strip()
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"DFX signer secret file not found: {normalized_path}"
        ) from exc

    if not value and not allow_empty:
        raise RuntimeError(
            f"DFX signer secret file is empty: {normalized_path}"
        )

    return value


def load_config():
    shared_secret = (
        os.environ.get("DFX_SIGNER_SHARED_SECRET", "").strip()
        or os.environ.get("MEDIACMS_INTERNAL_SHARED_SECRET", "").strip()
    )
    if not shared_secret:
        raise RuntimeError(
            "DFX_SIGNER_SHARED_SECRET or MEDIACMS_INTERNAL_SHARED_SECRET "
            "must be configured"
        )

    account_index = int(os.environ.get("DFX_SIGNER_ACCOUNT_INDEX", "0"))
    if account_index < 0:
        raise RuntimeError("DFX_SIGNER_ACCOUNT_INDEX must be >= 0")

    mnemonic_file = os.environ.get(
        "DFX_SIGNER_MNEMONIC_FILE",
        "/run/secrets/sweeper_evm_mnemonic",
    )
    mnemonic_passphrase_file = os.environ.get(
        "DFX_SIGNER_MNEMONIC_PASSPHRASE_FILE",
        "/run/secrets/sweeper_evm_mnemonic_passphrase",
    )

    return SimpleNamespace(
        shared_secret=shared_secret,
        mnemonic=_read_secret(mnemonic_file),
        mnemonic_passphrase=_read_secret(
            mnemonic_passphrase_file,
            allow_empty=True,
        ),
        account_index=account_index,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    server = start_dfx_signer_server(load_config())
    try:
        while True:
            time.sleep(3600)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
