from django.db import migrations


def backfill_observed_transfer_sessions(apps, schema_editor):
    DepositSession = apps.get_model("ledger", "DepositSession")
    ObservedOnchainTransfer = apps.get_model("ledger", "ObservedOnchainTransfer")

    for observed in ObservedOnchainTransfer.objects.filter(deposit_session__isnull=True).order_by("id"):
        qs = DepositSession.objects.filter(
            chain=observed.chain,
            asset_code=observed.asset_code,
            token_contract_address=observed.token_contract_address,
            deposit_address=observed.to_address,
        )

        if observed.txid:
            tx_match = qs.filter(observed_txid=observed.txid).order_by("-id").first()
            if tx_match is not None:
                observed.deposit_session_id = tx_match.id
                observed.save(update_fields=["deposit_session"])
                continue

        session = qs.order_by("-id").first()
        if session is None:
            raise RuntimeError(
                f"ObservedOnchainTransfer {observed.id} cannot be linked to a DepositSession"
            )

        observed.deposit_session_id = session.id
        observed.save(update_fields=["deposit_session"])


def reverse_backfill_observed_transfer_sessions(apps, schema_editor):
    ObservedOnchainTransfer = apps.get_model("ledger", "ObservedOnchainTransfer")
    ObservedOnchainTransfer.objects.update(deposit_session=None)


class Migration(migrations.Migration):

    dependencies = [
        ("ledger", "0019_observedonchaintransfer_confirmed_at_and_more"),
    ]

    operations = [
        migrations.RunPython(
            backfill_observed_transfer_sessions,
            reverse_backfill_observed_transfer_sessions,
        ),
    ]