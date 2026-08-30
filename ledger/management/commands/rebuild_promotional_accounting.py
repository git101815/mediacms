from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Max

from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet


PROMOTIONAL_ISSUANCE_KINDS = frozenset(
    {
        "daily_reward",
        "quest_reward",
        "referral_reward",
        "reward_chest",
    }
)
ACCOUNTED_TRANSACTION_STATUSES = (
    LedgerTransaction.STATUS_POSTED,
    LedgerTransaction.STATUS_REVERSED,
)
DEFAULT_BATCH_SIZE = 2000
MAX_CHANGE_EXAMPLES = 12


@dataclass
class ReconstructionStats:
    through_transaction_id: int
    transactions_scanned: int = 0
    user_entries_scanned: int = 0
    reversal_transactions: int = 0
    entry_updates: int = 0
    wallet_updates: int = 0
    wallets_clipped_to_current_balance: int = 0
    examples: list[str] = field(default_factory=list)

    def add_example(self, message: str) -> None:
        if len(self.examples) < MAX_CHANGE_EXAMPLES:
            self.examples.append(message)


def _validate_promotional_delta(*, delta: int, promotional_delta: int) -> None:
    if delta > 0 and not (0 <= promotional_delta <= delta):
        raise CommandError(
            f"Invalid reconstructed promotional credit: delta={delta}, "
            f"promotional_delta={promotional_delta}"
        )
    if delta < 0 and not (delta <= promotional_delta <= 0):
        raise CommandError(
            f"Invalid reconstructed promotional debit: delta={delta}, "
            f"promotional_delta={promotional_delta}"
        )


def _flush_entry_updates(rows: list[LedgerEntry], *, batch_size: int) -> None:
    if not rows:
        return
    LedgerEntry._base_manager.bulk_update(
        rows,
        ["promotional_delta"],
        batch_size=batch_size,
    )
    rows.clear()


def _flush_wallet_updates(rows: list[TokenWallet], *, batch_size: int) -> None:
    if not rows:
        return
    TokenWallet._base_manager.bulk_update(
        rows,
        ["promotional_balance"],
        batch_size=batch_size,
    )
    rows.clear()


def _reversal_source_buckets(
    source_rows: list[tuple[int, int, int]],
) -> dict[tuple[int, int], deque[int]]:
    buckets: dict[tuple[int, int], deque[int]] = defaultdict(deque)
    for wallet_id, delta, promotional_delta in source_rows:
        buckets[(wallet_id, delta)].append(promotional_delta)
    return buckets


def _allocate_component_proportionally(
    *,
    component_units: int,
    positive_entries: list[LedgerEntry],
) -> dict[int, int]:
    """Allocate one provenance component over credits without losing units."""
    component_units = max(0, int(component_units))
    positive_entries = [
        entry for entry in positive_entries if int(entry.delta) > 0
    ]
    total_positive = sum(int(entry.delta) for entry in positive_entries)
    if component_units == 0:
        return {int(entry.id): 0 for entry in positive_entries}
    if total_positive <= 0 or component_units > total_positive:
        raise CommandError("Cannot allocate reconstructed promotional provenance")

    allocations: dict[int, int] = {}
    cumulative = 0
    allocated = 0
    for entry in positive_entries:
        cumulative += int(entry.delta)
        target = (component_units * cumulative) // total_positive
        allocations[int(entry.id)] = target - allocated
        allocated = target

    if allocated != component_units:
        raise CommandError("Promotional provenance allocation lost token units")
    return allocations


def rebuild_promotional_accounting(
    *,
    apply: bool,
    through_transaction_id: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> ReconstructionStats:
    """Reconstruct free-token provenance from ledger history.

    A token issued for free stays promotional until it leaves a user wallet.
    Every ordinary outflow consumes paid/promotional units in the same
    proportion as the source wallet at that moment. If that outflow is split
    across several destinations, the promotional component is split across all
    credits in the same proportion as their amounts. Promotional value credited
    to a system wallet is considered consumed by the platform because system
    wallets do not carry promotional balances.

    Reversals restore the exact reconstructed provenance of the original
    transaction. This command only reconstructs provenance metadata/state; it
    never changes total token balances.
    """
    normalized_batch_size = int(batch_size)
    if normalized_batch_size <= 0:
        raise CommandError("batch_size must be greater than zero")

    if through_transaction_id is None:
        through_transaction_id = int(
            LedgerTransaction.objects.filter(
                status__in=ACCOUNTED_TRANSACTION_STATUSES,
            ).aggregate(max_id=Max("id"))["max_id"]
            or 0
        )
    else:
        through_transaction_id = int(through_transaction_id)
        if through_transaction_id < 0:
            raise CommandError("through_transaction_id must be >= 0")

    stats = ReconstructionStats(
        through_transaction_id=through_transaction_id,
    )
    if through_transaction_id == 0:
        return stats

    reversed_original_ids = set(
        LedgerTransaction.objects.filter(
            id__lte=through_transaction_id,
            status=LedgerTransaction.STATUS_REVERSED,
            reversal_of_id__isnull=False,
        ).values_list("reversal_of_id", flat=True)
    )

    promotional_by_wallet: dict[int, int] = {}
    reversal_sources: dict[int, list[tuple[int, int, int]]] = {}
    dirty_entries: list[LedgerEntry] = []

    queryset = (
        LedgerEntry._base_manager.filter(
            txn_id__lte=through_transaction_id,
            txn__status__in=ACCOUNTED_TRANSACTION_STATUSES,
        )
        .select_related("txn", "wallet")
        .order_by("txn__created_at", "txn_id", "id")
    )

    def set_desired(entry: LedgerEntry, desired: int) -> None:
        if entry.wallet.wallet_type != TokenWallet.TYPE_USER:
            return
        desired = int(desired)
        delta = int(entry.delta)
        _validate_promotional_delta(
            delta=delta,
            promotional_delta=desired,
        )
        stats.user_entries_scanned += 1
        if int(entry.promotional_delta) == desired:
            return
        stats.entry_updates += 1
        stats.add_example(
            f"entry {entry.id}: {int(entry.promotional_delta)} -> {desired}"
        )
        if apply:
            entry.promotional_delta = desired
            dirty_entries.append(entry)
            if len(dirty_entries) >= normalized_batch_size:
                _flush_entry_updates(
                    dirty_entries,
                    batch_size=normalized_batch_size,
                )

    def set_running_promotional(wallet_id: int, value: int) -> None:
        promotional_by_wallet[int(wallet_id)] = max(0, int(value))

    def process_transaction(entries: list[LedgerEntry]) -> None:
        if not entries:
            return

        stats.transactions_scanned += 1
        txn = entries[0].txn
        txn_id = int(txn.id)
        user_entries = [
            entry
            for entry in entries
            if entry.wallet.wallet_type == TokenWallet.TYPE_USER
        ]
        desired_rows: list[tuple[int, int, int]] = []
        reversal_of_id = int(txn.reversal_of_id) if txn.reversal_of_id else None

        if reversal_of_id is not None:
            stats.reversal_transactions += 1
            source_rows = reversal_sources.get(reversal_of_id)
            if source_rows is None:
                raise CommandError(
                    f"Cannot reconstruct reversal transaction {txn_id}: "
                    f"original transaction {reversal_of_id} was not replayed first"
                )
            source_buckets = _reversal_source_buckets(source_rows)

            for entry in user_entries:
                wallet_id = int(entry.wallet_id)
                delta = int(entry.delta)
                candidates = source_buckets.get((wallet_id, -delta))
                if not candidates:
                    raise CommandError(
                        f"Cannot match reversal entry {entry.id} to original "
                        f"transaction {reversal_of_id}"
                    )
                desired = -int(candidates.popleft())
                set_desired(entry, desired)
                current = int(promotional_by_wallet.get(wallet_id, 0))
                updated = current + desired
                if updated < 0:
                    raise CommandError(
                        f"Reversal would make wallet {wallet_id} promotional "
                        "provenance negative"
                    )
                set_running_promotional(wallet_id, updated)
                desired_rows.append((wallet_id, delta, desired))

            unmatched = sum(len(values) for values in source_buckets.values())
            if unmatched:
                raise CommandError(
                    f"Reversal transaction {txn_id} did not reverse all user "
                    f"entries from transaction {reversal_of_id}"
                )
            reversal_sources.pop(reversal_of_id, None)
            return

        promotional_outflow = 0

        # Debits consume provenance according to the source wallet's composition
        # immediately before that debit. ``balance_after`` lets the command infer
        # the historical total balance without changing any total-balance data.
        for entry in user_entries:
            delta = int(entry.delta)
            if delta >= 0:
                continue

            wallet_id = int(entry.wallet_id)
            debit_amount = abs(delta)
            balance_before = int(entry.balance_after) - delta
            if balance_before <= 0:
                raise CommandError(
                    f"Cannot reconstruct debit entry {entry.id}: "
                    f"non-positive source balance {balance_before}"
                )

            promotional_before = min(
                max(0, int(promotional_by_wallet.get(wallet_id, 0))),
                balance_before,
            )
            if debit_amount >= balance_before:
                promotional_spent = promotional_before
            else:
                promotional_spent = (
                    debit_amount * promotional_before
                ) // balance_before

            desired = -promotional_spent
            set_desired(entry, desired)
            set_running_promotional(
                wallet_id,
                promotional_before - promotional_spent,
            )
            promotional_outflow += promotional_spent
            desired_rows.append((wallet_id, delta, desired))

        positive_entries = [
            entry for entry in entries if int(entry.delta) > 0
        ]

        if str(txn.kind or "") in PROMOTIONAL_ISSUANCE_KINDS:
            allocations = {}
        else:
            allocations = _allocate_component_proportionally(
                component_units=promotional_outflow,
                positive_entries=positive_entries,
            )

        for entry in user_entries:
            delta = int(entry.delta)
            if delta <= 0:
                continue

            wallet_id = int(entry.wallet_id)
            if str(txn.kind or "") in PROMOTIONAL_ISSUANCE_KINDS:
                desired = delta
            else:
                desired = int(allocations.get(int(entry.id), 0))

            set_desired(entry, desired)
            current = int(promotional_by_wallet.get(wallet_id, 0))
            set_running_promotional(wallet_id, current + desired)
            desired_rows.append((wallet_id, delta, desired))

        if txn_id in reversed_original_ids:
            reversal_sources[txn_id] = desired_rows

    current_txn_id: int | None = None
    current_entries: list[LedgerEntry] = []
    for entry in queryset.iterator(chunk_size=normalized_batch_size):
        txn_id = int(entry.txn_id)
        if current_txn_id is None:
            current_txn_id = txn_id
        elif txn_id != current_txn_id:
            process_transaction(current_entries)
            current_entries = []
            current_txn_id = txn_id
        current_entries.append(entry)

    process_transaction(current_entries)

    if reversal_sources:
        unresolved = ", ".join(
            str(txn_id) for txn_id in sorted(reversal_sources)[:10]
        )
        raise CommandError(
            "Historical replay ended with unresolved reversal sources: "
            + unresolved
        )

    if apply:
        _flush_entry_updates(
            dirty_entries,
            batch_size=normalized_batch_size,
        )

    dirty_wallets: list[TokenWallet] = []
    wallets = (
        TokenWallet._base_manager.filter(wallet_type=TokenWallet.TYPE_USER)
        .only("id", "balance", "promotional_balance")
        .order_by("id")
    )
    for wallet in wallets.iterator(chunk_size=normalized_batch_size):
        reconstructed = max(
            0,
            int(promotional_by_wallet.get(int(wallet.id), 0)),
        )
        positive_balance = max(0, int(wallet.balance))
        desired = min(reconstructed, positive_balance)
        if reconstructed > positive_balance:
            stats.wallets_clipped_to_current_balance += 1

        if int(wallet.promotional_balance) == desired:
            continue

        stats.wallet_updates += 1
        stats.add_example(
            f"wallet {wallet.id}: {int(wallet.promotional_balance)} -> {desired}"
        )
        if apply:
            wallet.promotional_balance = desired
            dirty_wallets.append(wallet)
            if len(dirty_wallets) >= normalized_batch_size:
                _flush_wallet_updates(
                    dirty_wallets,
                    batch_size=normalized_batch_size,
                )

    if apply:
        _flush_wallet_updates(
            dirty_wallets,
            batch_size=normalized_batch_size,
        )

    return stats


class Command(BaseCommand):
    help = (
        "Reconstruct historical free-token provenance. Defaults to dry-run; "
        "pass --apply to write promotional deltas and wallet promo balances."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Write reconstructed promotional provenance.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Explicitly request the default read-only mode.",
        )
        parser.add_argument(
            "--through-transaction-id",
            type=int,
            default=None,
            help=(
                "Only replay accounted ledger transactions up to this ID. "
                "By default the command snapshots the current maximum ID."
            ),
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help=f"Database iterator/update batch size (default: {DEFAULT_BATCH_SIZE}).",
        )

    def handle(self, *args, **options):
        if options["apply"] and options["dry_run"]:
            raise CommandError("Choose either --apply or --dry-run, not both")

        applying = bool(options["apply"])
        through_transaction_id = options["through_transaction_id"]
        batch_size = options["batch_size"]

        if applying:
            # Runtime ledger writes should be paused for this one-off repair so
            # the reconstructed entry provenance and final wallet state are one
            # consistent snapshot.
            with transaction.atomic():
                stats = rebuild_promotional_accounting(
                    apply=True,
                    through_transaction_id=through_transaction_id,
                    batch_size=batch_size,
                )
        else:
            stats = rebuild_promotional_accounting(
                apply=False,
                through_transaction_id=through_transaction_id,
                batch_size=batch_size,
            )

        mode = "APPLY" if applying else "DRY-RUN"
        self.stdout.write(
            f"[{mode}] through transaction id: {stats.through_transaction_id}"
        )
        self.stdout.write(
            f"transactions scanned: {stats.transactions_scanned}"
        )
        self.stdout.write(
            f"user entries scanned: {stats.user_entries_scanned}"
        )
        self.stdout.write(
            f"reversal transactions: {stats.reversal_transactions}"
        )
        self.stdout.write(
            f"ledger entries to change: {stats.entry_updates}"
        )
        self.stdout.write(f"wallets to change: {stats.wallet_updates}")
        self.stdout.write(
            "wallets clipped to current balance: "
            f"{stats.wallets_clipped_to_current_balance}"
        )
        if stats.examples:
            self.stdout.write("examples:")
            for example in stats.examples:
                self.stdout.write(f"  - {example}")

        if applying:
            self.stdout.write(
                self.style.SUCCESS("promotional accounting rebuilt")
            )
        else:
            self.stdout.write(
                self.style.WARNING(
                    "dry-run only; rerun with --apply to write these changes"
                )
            )
