import os
import sys
import time
from urllib.parse import urljoin

import httpx


def main() -> int:
    base_url = os.environ.get("MEDIACMS_INTERNAL_BASE_URL", "").strip()
    if not base_url:
        print("wait_for_web error=missing_base_url env=MEDIACMS_INTERNAL_BASE_URL", flush=True)
        return 1

    probe_path = os.environ.get("MEDIACMS_INTERNAL_READY_PATH", "/").strip() or "/"
    probe_url = urljoin(base_url.rstrip("/") + "/", probe_path.lstrip("/"))

    timeout_seconds = float(os.environ.get("MEDIACMS_INTERNAL_READY_TIMEOUT_SECONDS", "5"))
    max_wait_seconds = float(os.environ.get("MEDIACMS_INTERNAL_READY_MAX_WAIT_SECONDS", "180"))
    sleep_seconds = float(os.environ.get("MEDIACMS_INTERNAL_READY_RETRY_SECONDS", "2"))

    deadline = time.monotonic() + max_wait_seconds
    last_error = None

    with httpx.Client(timeout=timeout_seconds) as client:
        while time.monotonic() < deadline:
            try:
                response = client.get(probe_url)
                if 200 <= response.status_code < 500:
                    print(
                        f"wait_for_web status=ready url={probe_url} code={response.status_code}",
                        flush=True,
                    )
                    return 0

                last_error = f"unexpected_status:{response.status_code}"
                print(
                    f"wait_for_web status=retry url={probe_url} code={response.status_code}",
                    flush=True,
                )
            except Exception as exc:
                last_error = str(exc)
                print(
                    f"wait_for_web status=retry url={probe_url} error={last_error}",
                    flush=True,
                )

            time.sleep(sleep_seconds)

    print(
        f"wait_for_web status=timeout url={probe_url} last_error={last_error}",
        flush=True,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())