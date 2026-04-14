import logging
import time
from dataclasses import dataclass

from .evm_rpc import build_web3


@dataclass(frozen=True)
class RpcProbeResult:
    rpc_url: str
    latest_block: int
    latency_seconds: float


def _probe_rpc(
    *,
    rpc_url: str,
    poa_compatible: bool,
) -> RpcProbeResult:
    started = time.monotonic()
    w3 = build_web3(rpc_url=rpc_url, poa_compatible=poa_compatible)
    latest_block = int(w3.eth.block_number)
    latency_seconds = time.monotonic() - started
    return RpcProbeResult(
        rpc_url=rpc_url,
        latest_block=latest_block,
        latency_seconds=latency_seconds,
    )


def choose_best_rpc_url(
    *,
    option_key: str,
    rpc_urls: list[str],
    poa_compatible: bool,
    max_lag_blocks: int,
) -> str:
    if not rpc_urls:
        raise RuntimeError(f"No RPC URLs configured for option {option_key}")

    successes: list[RpcProbeResult] = []
    failures: list[tuple[str, str]] = []

    for rpc_url in rpc_urls:
        try:
            result = _probe_rpc(
                rpc_url=rpc_url,
                poa_compatible=poa_compatible,
            )
            successes.append(result)
        except Exception as exc:
            failures.append((rpc_url, str(exc)))
            logging.warning(
                "rpc probe failed option=%s rpc=%s error=%s",
                option_key,
                rpc_url,
                exc,
            )

    if not successes:
        raise RuntimeError(
            f"All RPC probes failed for option {option_key}: "
            + ", ".join(f"{url} -> {error}" for url, error in failures)
        )

    best_head = max(item.latest_block for item in successes)
    healthy = [
        item
        for item in successes
        if (best_head - item.latest_block) <= int(max_lag_blocks)
    ]

    if not healthy:
        raise RuntimeError(
            f"All RPC endpoints are too far behind for option {option_key}; "
            f"best_head={best_head}, max_lag_blocks={max_lag_blocks}"
        )

    healthy.sort(
        key=lambda item: (
            -item.latest_block,
            item.latency_seconds,
        )
    )
    chosen = healthy[0]

    lagging = [
        item for item in successes if item.rpc_url != chosen.rpc_url
    ]
    for item in lagging:
        lag = best_head - item.latest_block
        if lag > int(max_lag_blocks):
            logging.warning(
                "rpc excluded as unhealthy option=%s rpc=%s latest_block=%s best_head=%s lag=%s max_lag_blocks=%s",
                option_key,
                item.rpc_url,
                item.latest_block,
                best_head,
                lag,
                max_lag_blocks,
            )

    logging.info(
        "rpc selected option=%s rpc=%s latest_block=%s best_head=%s latency_ms=%s",
        option_key,
        chosen.rpc_url,
        chosen.latest_block,
        best_head,
        int(chosen.latency_seconds * 1000),
    )
    return chosen.rpc_url