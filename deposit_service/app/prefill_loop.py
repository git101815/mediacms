import logging
import time

from .client import MediaCMSInternalClient
from .config import load_config
from .observe_once import observe_once

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
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
                observe_once(
                    client=client,
                    options=config.options,
                    rpc_max_lag_blocks=config.rpc_max_lag_blocks,
                )
            except Exception:
                logging.exception("deposit_service cycle failed")
            time.sleep(config.poll_interval_seconds)
    finally:
        client.close()


if __name__ == "__main__":
    main()