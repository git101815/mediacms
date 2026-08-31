from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Max

from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet


RESTRICTED_ISSUANCE_KINDS = frozenset(
    {"daily_reward", "quest_reward", "referral_reward", "reward_chest"}
)
ECONOMIC_CONVERSION_PRODUCTS = frozenset(
    {"premium_media", "creator_subscription"}
)
ACCOUNTED_STATUSES = (
    LedgerTransaction.STATUS_POSTED,
    LedgerTransaction.STATUS_REVERSED,
)
DEFAULT_BATCH_SIZE = 2000
MAX_EXAMPLES = 12


@dataclass
class Stats:
    through_transaction_id: int
    transactions_scanned: int = 0
    user_entries_scanned: int = 0
    reversal_transactions: int = 0
    entry_updates: int = 0
    wallet_updates: int = 0
    examples: list[str] = field(default_factory=list)

    def example(self, value: str) -> None:
        if len(self.examples) < MAX_EXAMPLES:
            self.examples.append(value)


def _validate(delta: int, promotional_delta: int, restricted_delta: int) -> None:
    if delta > 0 and not (0 <= restricted_delta <= promotional_delta <= delta):
        raise CommandError("Invalid reconstructed restricted promotional credit")
    if delta < 0 and not (delta <= promotional_delta <= restricted_delta <= 0):
        raise CommandError("Invalid reconstructed restricted promotional debit")


def _is_economic_conversion(txn: LedgerTransaction) -> bool:
    metadata = txn.metadata or {}
    return str(metadata.get("product") or "") in ECONOMIC_CONVERSION_PRODUCTS


def _reversal_buckets(rows):
    buckets = defaultdict(deque)
    for wallet_id, delta, promo, restricted in rows:
        buckets[(wallet_id, delta, promo)].append(restricted)
    return buckets


def _allocate_component(component_units: int, weighted_entries):
    component_units = max(0, int(component_units))
    weighted = [(int(key), int(weight)) for key, weight in weighted_entries if int(weight) > 0]
    total = sum(weight for _key, weight in weighted)
    if component_units == 0:
        return {key: 0 for key, _weight in weighted}
    if total <= 0 or component_units > total:
        raise CommandError("Cannot allocate restricted promotional provenance")
    result = {}
    cumulative = 0
    allocated = 0
    for key, weight in weighted:
        cumulative += weight
        target = (component_units * cumulative) // total
        result[key] = target - allocated
        allocated = target
    if allocated != component_units:
        raise CommandError("Restricted promotional allocation lost units")
    return result


def rebuild_restricted_promotional_accounting(
    *,
    apply: bool,
    through_transaction_id: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Stats:
    batch_size = int(batch_size)
    if batch_size <= 0:
        raise CommandError("batch_size must be greater than zero")

    current_max = int(
        LedgerTransaction.objects.filter(status__in=ACCOUNTED_STATUSES)
        .aggregate(max_id=Max("id"))["max_id"]
        or 0
    )
    if through_transaction_id is None:
        through_transaction_id = current_max
    else:
        through_transaction_id = int(through_transaction_id)
        if through_transaction_id < 0:
            raise CommandError("through_transaction_id must be >= 0")
        if apply and through_transaction_id != current_max:
            raise CommandError(
                "Refusing to apply a stale transaction cutoff; rerun without "
                "--through-transaction-id"
            )

    stats = Stats(through_transaction_id=through_transaction_id)
    if through_transaction_id == 0:
        return stats

    reversed_original_ids = set(
        LedgerTransaction.objects.filter(
            id__lte=through_transaction_id,
            status=LedgerTransaction.STATUS_REVERSED,
            reversal_of_id__isnull=False,
        ).values_list("reversal_of_id", flat=True)
    )

    promo_by_wallet: dict[int, int] = {}
    restricted_by_wallet: dict[int, int] = {}
    reversal_sources = {}
    dirty_entries = []

    queryset = (
        LedgerEntry._base_manager.filter(
            txn_id__lte=through_transaction_id,
            txn__status__in=ACCOUNTED_STATUSES,
        )
        .select_related("txn", "wallet")
        .order_by("txn__created_at", "txn_id", "id")
    )

    def set_entry(entry, desired):
        if entry.wallet.wallet_type != TokenWallet.TYPE_USER:
            return
        desired = int(desired)
        promo = int(entry.promotional_delta)
        _validate(int(entry.delta), promo, desired)
        stats.user_entries_scanned += 1
        if int(entry.restricted_promotional_delta) == desired:
            return
        stats.entry_updates += 1
        stats.example(
            f"entry {entry.id}: {int(entry.restricted_promotional_delta)} -> {desired}"
        )
        if apply:
            entry.restricted_promotional_delta = desired
            dirty_entries.append(entry)
            if len(dirty_entries) >= batch_size:
                LedgerEntry._base_manager.bulk_update(
                    dirty_entries,
                    ["restricted_promotional_delta"],
                    batch_size=batch_size,
                )
                dirty_entries.clear()

    def process(entries):
        if not entries:
            return
        stats.transactions_scanned += 1
        txn = entries[0].txn
        txn_id = int(txn.id)
        users = [e for e in entries if e.wallet.wallet_type == TokenWallet.TYPE_USER]
        desired_rows = []

        if txn.reversal_of_id:
            stats.reversal_transactions += 1
            source = reversal_sources.get(int(txn.reversal_of_id))
            if source is None:
                raise CommandError(
                    f"Cannot reconstruct reversal {txn_id}: original was not replayed"
                )
            buckets = _reversal_buckets(source)
            for entry in users:
                wallet_id = int(entry.wallet_id)
                key = (wallet_id, -int(entry.delta), -int(entry.promotional_delta))
                values = buckets.get(key)
                if not values:
                    raise CommandError(
                        f"Cannot match reversal entry {entry.id} to original"
                    )
                desired = -int(values.popleft())
                set_entry(entry, desired)
                promo_by_wallet[wallet_id] = max(
                    0,
                    int(promo_by_wallet.get(wallet_id, 0))
                    + int(entry.promotional_delta),
                )
                updated_restricted = (
                    int(restricted_by_wallet.get(wallet_id, 0)) + desired
                )
                if updated_restricted < 0:
                    raise CommandError("Reversal makes restricted provenance negative")
                restricted_by_wallet[wallet_id] = updated_restricted
                desired_rows.append(
                    (wallet_id, int(entry.delta), int(entry.promotional_delta), desired)
                )
            if sum(len(v) for v in buckets.values()):
                raise CommandError(f"Reversal {txn_id} did not match all original entries")
            reversal_sources.pop(int(txn.reversal_of_id), None)
            return

        restricted_outflow = 0
        promotional_outflow = 0

        for entry in users:
            delta = int(entry.delta)
            if delta >= 0:
                continue
            wallet_id = int(entry.wallet_id)
            promo_delta = int(entry.promotional_delta)
            promo_spent = max(0, -promo_delta)
            promo_before = max(0, int(promo_by_wallet.get(wallet_id, 0)))
            restricted_before = min(
                max(0, int(restricted_by_wallet.get(wallet_id, 0))),
                promo_before,
            )
            if promo_spent > promo_before:
                raise CommandError(
                    f"Entry {entry.id} spends more promotional provenance than available"
                )
            if promo_spent == 0 or restricted_before == 0:
                restricted_spent = 0
            elif promo_spent >= promo_before:
                restricted_spent = restricted_before
            else:
                restricted_spent = (promo_spent * restricted_before) // promo_before

            desired = -restricted_spent
            set_entry(entry, desired)
            promo_by_wallet[wallet_id] = promo_before - promo_spent
            restricted_by_wallet[wallet_id] = restricted_before - restricted_spent
            promotional_outflow += promo_spent
            restricted_outflow += restricted_spent
            desired_rows.append((wallet_id, delta, promo_delta, desired))

        positive_entries = [e for e in entries if int(e.delta) > 0]
        positive_users = [e for e in users if int(e.delta) > 0]
        economic_conversion = _is_economic_conversion(txn)

        promotional_allocations = {}
        restricted_allocations = {}
        if (
            str(txn.kind or "") not in RESTRICTED_ISSUANCE_KINDS
            and not economic_conversion
            and promotional_outflow > 0
        ):
            promotional_allocations = _allocate_component(
                promotional_outflow,
                [(int(e.id), int(e.delta)) for e in positive_entries],
            )
            # Runtime generic ledger transfers use this exact two-stage
            # allocation: promo follows total credits, then restricted follows
            # the promotional allocation (including the system-consumed share).
            restricted_allocations = _allocate_component(
                restricted_outflow,
                [
                    (int(e.id), int(promotional_allocations.get(int(e.id), 0)))
                    for e in positive_entries
                ],
            )

        for entry in positive_users:
            wallet_id = int(entry.wallet_id)
            promo_delta = max(0, int(entry.promotional_delta))
            if str(txn.kind or "") in RESTRICTED_ISSUANCE_KINDS:
                desired = promo_delta
            elif economic_conversion:
                desired = 0
            elif promotional_outflow <= 0 or restricted_outflow <= 0:
                desired = 0
            else:
                expected_promo = int(promotional_allocations.get(int(entry.id), 0))
                if expected_promo != promo_delta:
                    raise CommandError(
                        f"Transaction {txn_id} promotional allocation does not match "
                        f"entry {entry.id}; rebuild base promotional accounting first"
                    )
                desired = int(restricted_allocations.get(int(entry.id), 0))

            set_entry(entry, desired)
            promo_by_wallet[wallet_id] = (
                int(promo_by_wallet.get(wallet_id, 0)) + promo_delta
            )
            restricted_by_wallet[wallet_id] = (
                int(restricted_by_wallet.get(wallet_id, 0)) + desired
            )
            desired_rows.append(
                (wallet_id, int(entry.delta), int(entry.promotional_delta), desired)
            )

        if txn_id in reversed_original_ids:
            reversal_sources[txn_id] = desired_rows

    current_txn = None
    entries = []
    for entry in queryset.iterator(chunk_size=batch_size):
        txn_id = int(entry.txn_id)
        if current_txn is None:
            current_txn = txn_id
        elif txn_id != current_txn:
            process(entries)
            entries = []
            current_txn = txn_id
        entries.append(entry)
    process(entries)

    if reversal_sources:
        raise CommandError(
            "Historical replay ended with unresolved reversal sources: "
            + ", ".join(str(v) for v in sorted(reversal_sources)[:10])
        )

    if apply and dirty_entries:
        LedgerEntry._base_manager.bulk_update(
            dirty_entries,
            ["restricted_promotional_delta"],
            batch_size=batch_size,
        )

    dirty_wallets = []
    wallets = (
        TokenWallet._base_manager.filter(wallet_type=TokenWallet.TYPE_USER)
        .only(
            "id",
            "promotional_balance",
            "restricted_promotional_balance",
        )
        .order_by("id")
    )
    for wallet in wallets.iterator(chunk_size=batch_size):
        promo = max(0, int(wallet.promotional_balance))
        desired = min(
            max(0, int(restricted_by_wallet.get(int(wallet.id), 0))),
            promo,
        )
        if int(wallet.restricted_promotional_balance) == desired:
            continue
        stats.wallet_updates += 1
        stats.example(
            f"wallet {wallet.id}: restricted "
            f"{int(wallet.restricted_promotional_balance)} -> {desired}"
        )
        if apply:
            wallet.restricted_promotional_balance = desired
            dirty_wallets.append(wallet)
            if len(dirty_wallets) >= batch_size:
                TokenWallet._base_manager.bulk_update(
                    dirty_wallets,
                    ["restricted_promotional_balance"],
                    batch_size=batch_size,
                )
                dirty_wallets.clear()

    if apply and dirty_wallets:
        TokenWallet._base_manager.bulk_update(
            dirty_wallets,
            ["restricted_promotional_balance"],
            batch_size=batch_size,
        )
    return stats


class Command(BaseCommand):
    help = (
        "Reconstruct restricted promotional provenance after the schema migration. "
        "Defaults to dry-run."
    )

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--through-transaction-id", type=int, default=None)
        parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)

    def handle(self, *args, **options):
        if options["apply"] and options["dry_run"]:
            raise CommandError("Choose either --apply or --dry-run, not both")
        applying = bool(options["apply"])

        if applying:
            with transaction.atomic():
                # Lock every user wallet first. Runtime flows already lock their
                # wallet before mutating provenance, so this creates one stable
                # replay snapshot instead of repeating the stale-cutoff problem.
                list(
                    TokenWallet.objects.select_for_update()
                    .filter(wallet_type=TokenWallet.TYPE_USER)
                    .order_by("id")
                    .values_list("id", flat=True)
                )
                stats = rebuild_restricted_promotional_accounting(
                    apply=True,
                    through_transaction_id=options["through_transaction_id"],
                    batch_size=options["batch_size"],
                )
        else:
            stats = rebuild_restricted_promotional_accounting(
                apply=False,
                through_transaction_id=options["through_transaction_id"],
                batch_size=options["batch_size"],
            )

        mode = "APPLY" if applying else "DRY-RUN"
        self.stdout.write(f"[{mode}] through transaction id: {stats.through_transaction_id}")
        self.stdout.write(f"transactions scanned: {stats.transactions_scanned}")
        self.stdout.write(f"user entries scanned: {stats.user_entries_scanned}")
        self.stdout.write(f"reversal transactions: {stats.reversal_transactions}")
        self.stdout.write(f"ledger entries to change: {stats.entry_updates}")
        self.stdout.write(f"wallets to change: {stats.wallet_updates}")
        if stats.examples:
            self.stdout.write("examples:")
            for example in stats.examples:
                self.stdout.write(f"  - {example}")
        if applying:
            self.stdout.write(self.style.SUCCESS("restricted promotional accounting rebuilt"))
        else:
            self.stdout.write(
                self.style.WARNING("dry-run only; rerun with --apply to write these changes")
            )
