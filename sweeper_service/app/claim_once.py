import logging
import time

from .client import MediaCMSInternalClient
from .config import load_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def _build_option_selector(option) -> dict:
    return {
        "chain": option.chain,
        "asset_code": option.asset_code,
        "token_contract_address": option.token_contract_address,
    }


def run_once(*, client: MediaCMSInternalClient, config) -> None:
    options = [_build_option_selector(option) for option in config.options]
    jobs = client.claim_jobs(
        options=options,
        limit=config.claim_batch_size,
    )

    if not jobs:
        logging.info("sweeper_service action=noop claimed=0")
        return

    for job in jobs:
        logging.info(
            "sweeper_service action=claimed public_id=%s chain=%s asset=%s source_address=%s amount=%s status=%s",
            job["public_id"],
            job["chain"],
            job["asset_code"],
            job["source_address"],
            job["amount"],
            job["status"],
        )


def main() -> None:
    config = load_config()
    client = MediaCMSInternalClient(
        base_url=config.mediacms_base_url,
        service_name=config.service_name,
        shared_secret=config.shared_secret,
    )

    try:
        while True:
            try:
                run_once(client=client, config=config)
            except Exception:
                logging.exception("sweeper_service cycle failed")
            time.sleep(config.poll_interval_seconds)
    finally:
        client.close()


if __name__ == "__main__":
    main()