from django.db import migrations


PROMOTIONAL_ISSUANCE_KINDS = frozenset(
    {
        "daily_reward",
        "quest_reward",
        "referral_reward",
        "reward_chest",
    }
)

# Reversal transactions contain real ledger entries even though their status is
# "reversed"; pending transactions have not affected wallet balances.
ACCOUNTED_TRANSACTION_STATUSES = ("posted", "reversed")
BATCH_SIZE = 2000


def _flush_entries(LedgerEntry, rows):
    if not rows:
        return
    LedgerEntry._base_manager.bulk_update(
        rows,
        ["promotional_delta"],
        batch_size=BATCH_SIZE,
    )
    rows.clear()


def _flush_wallets(TokenWallet, rows):
    if not rows:
        return
    TokenWallet._base_manager.bulk_update(
        rows,
        ["promotional_balance"],
        batch_size=BATCH_SIZE,
    )
    rows.clear()


def backfill_promotional_accounting(apps, schema_editor):
    TokenWallet = apps.get_model("ledger", "TokenWallet")
    LedgerEntry = apps.get_model("ledger", "LedgerEntry")

    # 0038 created both provenance columns with a zero default. Do not issue a
    # table-wide UPDATE here: on a large ledger that would create unnecessary
    # write amplification and locking. This migration only writes rows whose
    # reconstructed promotional component is non-zero.
    promotional_by_wallet = {}
    dirty_entries = []

    # Process one ledger transaction at a time, in ledger chronology. Within a
    # transaction, user debits are processed before user credits so promotional
    # provenance follows value transferred between user wallets.
    queryset = (
        LedgerEntry._base_manager.filter(
            wallet__wallet_type="user",
            txn__status__in=ACCOUNTED_TRANSACTION_STATUSES,
        )
        .select_related("txn")
        .order_by("txn__created_at", "txn_id", "id")
    )

    current_txn_id = None
    current_entries = []

    def process_transaction(entries):
        if not entries:
            return

        kind = str(entries[0].txn.kind or "")
        promotional_to_propagate = 0

        # Historical policy is reconstructed as promo-first spending. This
        # reflects the runtime policy introduced with promotional accounting
        # and prevents a previous debit from leaving already-spent rewards in
        # the current promotional bucket.
        for entry in entries:
            delta = int(entry.delta)
            if delta >= 0:
                continue

            wallet_id = int(entry.wallet_id)
            available_promotional = max(
                0,
                int(promotional_by_wallet.get(wallet_id, 0)),
            )
            consumed = min(available_promotional, abs(delta))
            if consumed:
                entry.promotional_delta = -consumed
                promotional_by_wallet[wallet_id] = (
                    available_promotional - consumed
                )
                promotional_to_propagate += consumed
                dirty_entries.append(entry)

        for entry in entries:
            delta = int(entry.delta)
            if delta <= 0:
                continue

            wallet_id = int(entry.wallet_id)

            if kind in PROMOTIONAL_ISSUANCE_KINDS:
                promotional_credit = delta
            else:
                # Any promotional value debited from another user in the same
                # transaction remains promotional on the receiving user. This
                # reconstructs old user-to-user purchases/transfers without
                # turning rewards into cash-outable seller proceeds.
                promotional_credit = min(
                    promotional_to_propagate,
                    delta,
                )
                promotional_to_propagate -= promotional_credit

            if promotional_credit:
                entry.promotional_delta = promotional_credit
                promotional_by_wallet[wallet_id] = (
                    int(promotional_by_wallet.get(wallet_id, 0))
                    + promotional_credit
                )
                dirty_entries.append(entry)

        if len(dirty_entries) >= BATCH_SIZE:
            _flush_entries(LedgerEntry, dirty_entries)

    for entry in queryset.iterator(chunk_size=BATCH_SIZE):
        txn_id = int(entry.txn_id)
        if current_txn_id is None:
            current_txn_id = txn_id
        elif txn_id != current_txn_id:
            process_transaction(current_entries)
            current_entries = []
            current_txn_id = txn_id
        current_entries.append(entry)

    process_transaction(current_entries)
    _flush_entries(LedgerEntry, dirty_entries)

    dirty_wallets = []
    for wallet in (
        TokenWallet._base_manager.filter(wallet_type="user")
        .only("id", "balance", "promotional_balance")
        .order_by("id")
        .iterator(chunk_size=BATCH_SIZE)
    ):
        reconstructed = max(
            0,
            int(promotional_by_wallet.get(int(wallet.id), 0)),
        )

        # Legacy code occasionally mutated a wallet outside the generic ledger
        # helper. If such a historical discrepancy exists, classify at most the
        # entire current positive balance as promotional. This is conservative:
        # it can reduce cash-out eligibility but can never manufacture
        # withdrawable funds.
        positive_balance = max(0, int(wallet.balance))
        reconstructed = min(reconstructed, positive_balance)
        if reconstructed <= 0:
            continue

        wallet.promotional_balance = reconstructed
        dirty_wallets.append(wallet)

        if len(dirty_wallets) >= BATCH_SIZE:
            _flush_wallets(TokenWallet, dirty_wallets)

    _flush_wallets(TokenWallet, dirty_wallets)


class Migration(migrations.Migration):

    dependencies = [
        ("ledger", "0038_ledgerentry_promotional_delta_and_more"),
    ]

    operations = [
        migrations.RunPython(
            backfill_promotional_accounting,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
