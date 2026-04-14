import logging
import time

from .client import MediaCMSInternalClient
from .config import load_config
from .observe_once import observe_once


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    while True:
        client = None
        try:
            config = load_config()
            client = MediaCMSInternalClient(
                base_url=config.mediacms_base_url,
                service_name=config.service_name,
                shared_secret=config.shared_secret,
            )

            observe_once(
                client=client,
                options=config.options,
                reference_heads_base_url=config.reference_heads_base_url,
                reference_heads_shared_secret=config.reference_heads_shared_secret,
                reference_heads_timeout_seconds=config.reference_heads_timeout_seconds,
                reference_heads_max_age_seconds=config.reference_heads_max_age_seconds,
                rpc_max_lag_blocks=config.rpc_max_lag_blocks,
                rpc_max_reference_lag_blocks=config.rpc_max_reference_lag_blocks,
            )
        except Exception:
            logging.exception("deposit_service cycle failed")
        finally:
            if client is not None:
                client.close()

        time.sleep(30)


if __name__ == "__main__":
    main()