import logging
import uuid
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver
from django.db.utils import ProgrammingError, OperationalError

from .models import (
    LEDGER_ACTION_PURCHASE,
    DepositSweepJob,
    LedgerOutbox,
    LedgerSaga,
    LedgerTransaction,
    P2POrder,
    TokenWallet,
    WalletRequest,
)


logger = logging.getLogger(__name__)
User = get_user_model()


def _remember_previous_value(model, instance, field_name, attr_name):
    if not instance.pk:
        setattr(instance, attr_name, None)
        return

    value = (
        model.objects
        .filter(pk=instance.pk)
        .values_list(field_name, flat=True)
        .first()
    )
    setattr(instance, attr_name, value)


def _format_scaled_amount(value):
    try:
        scaled = Decimal(int(value)) / Decimal(10 ** 6)
    except (TypeError, ValueError):
        return ""
    text = format(scaled, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _user_payload(user):
    if user is None:
        return {}
    return {
        "id": getattr(user, "pk", None),
        "username": str(getattr(user, "username", "") or ""),
        "email": str(getattr(user, "email", "") or ""),
    }


def _safe_enqueue_notification(event, payload, event_id):
    try:
        from .tasks import notify_admin_event

        notify_admin_event.delay(
            event,
            payload,
            event_id,
        )
    except Exception:
        # Notifications are operational side effects. A broker/n8n problem
        # must never turn an already-committed financial operation into a 500.
        logger.exception(
            "Failed to enqueue admin notification %s",
            event,
        )


def _queue_admin_notification(event, payload):
    event_id = uuid.uuid4().hex
    transaction.on_commit(
        lambda: _safe_enqueue_notification(
            event,
            payload,
            event_id,
        )
    )


@receiver(post_save, sender=User)
def ensure_token_wallet(sender, instance, created, **kwargs):
    try:
        TokenWallet.objects.get_or_create(
            user=instance,
            defaults={
                "wallet_type": TokenWallet.TYPE_USER,
                "allow_negative": False,
            },
        )
    except (ProgrammingError, OperationalError):
        return


@receiver(post_save, sender=LedgerTransaction)
def queue_referral_reward_for_purchase(sender, instance, created, **kwargs):
    if not created:
        return
    if (
        instance.kind != LEDGER_ACTION_PURCHASE
        or instance.status != LedgerTransaction.STATUS_POSTED
    ):
        return

    purchase_txn_id = instance.pk

    def process_after_commit():
        from ledger.dashboard.referrals import (
            safely_award_referral_for_purchase,
        )

        safely_award_referral_for_purchase(
            purchase_txn_id=purchase_txn_id,
        )

    transaction.on_commit(process_after_commit)


@receiver(pre_save, sender=WalletRequest)
def wallet_request_before_save(sender, instance, **kwargs):
    _remember_previous_value(
        WalletRequest,
        instance,
        "status",
        "_notification_previous_status",
    )


@receiver(post_save, sender=WalletRequest)
def wallet_request_review_requested(
    sender,
    instance,
    created=False,
    **kwargs,
):
    previous = getattr(
        instance,
        "_notification_previous_status",
        None,
    )
    if (
        instance.status != WalletRequest.STATUS_PENDING
        or (not created and previous == WalletRequest.STATUS_PENDING)
    ):
        return

    try:
        user = instance.wallet.user
    except Exception:
        user = None

    amount_display = _format_scaled_amount(instance.amount)
    if amount_display:
        amount_display = (
            f"{amount_display} {instance.asset_code}"
        )

    _queue_admin_notification(
        "wallet.review_requested",
        {
            "severity": "review",
            "title": "Wallet request needs review",
            "object_type": "wallet_request",
            "object_id": instance.pk,
            "reference": instance.reference,
            "request_type": instance.request_type,
            "amount": int(instance.amount),
            "amount_display": amount_display,
            "asset_code": instance.asset_code,
            "destination": instance.destination_address,
            "notes": instance.notes,
            "wallet_id": instance.wallet_id,
            "user": _user_payload(user),
            "admin_url": (
                f"/admin/ledger/walletrequest/{instance.pk}/change/"
            ),
        },
    )


@receiver(pre_save, sender=LedgerOutbox)
def ledger_outbox_before_save(sender, instance, **kwargs):
    _remember_previous_value(
        LedgerOutbox,
        instance,
        "status",
        "_notification_previous_status",
    )


@receiver(post_save, sender=LedgerOutbox)
def ledger_outbox_dead_lettered(
    sender,
    instance,
    created=False,
    **kwargs,
):
    previous = getattr(
        instance,
        "_notification_previous_status",
        None,
    )
    if (
        instance.status != LedgerOutbox.STATUS_DEAD_LETTERED
        or (
            not created
            and previous == LedgerOutbox.STATUS_DEAD_LETTERED
        )
    ):
        return

    try:
        txn_external_id = instance.txn.external_id or ""
    except Exception:
        txn_external_id = ""

    _queue_admin_notification(
        "ledger.outbox_dead_lettered",
        {
            "severity": "critical",
            "title": "Ledger outbox dead-lettered",
            "object_type": "ledger_outbox",
            "object_id": instance.pk,
            "topic": instance.topic,
            "aggregate_type": instance.aggregate_type,
            "aggregate_id": instance.aggregate_id,
            "txn_id": instance.txn_id,
            "txn_external_id": txn_external_id,
            "fail_count": int(instance.fail_count or 0),
            "error": (
                instance.dead_letter_reason
                or instance.last_error
                or ""
            ),
            "admin_url": (
                f"/admin/ledger/ledgeroutbox/{instance.pk}/change/"
            ),
        },
    )


@receiver(pre_save, sender=LedgerSaga)
def ledger_saga_before_save(sender, instance, **kwargs):
    _remember_previous_value(
        LedgerSaga,
        instance,
        "status",
        "_notification_previous_status",
    )


@receiver(post_save, sender=LedgerSaga)
def ledger_saga_failed(
    sender,
    instance,
    created=False,
    **kwargs,
):
    previous = getattr(
        instance,
        "_notification_previous_status",
        None,
    )
    if (
        instance.status != LedgerSaga.STATUS_FAILED
        or (not created and previous == LedgerSaga.STATUS_FAILED)
    ):
        return

    try:
        user = instance.created_by
    except Exception:
        user = None

    _queue_admin_notification(
        "ledger.saga_failed",
        {
            "severity": "critical",
            "title": "Ledger saga failed",
            "object_type": "ledger_saga",
            "object_id": instance.pk,
            "saga_type": instance.saga_type,
            "external_id": instance.external_id or "",
            "error": instance.last_error or "",
            "user": _user_payload(user),
            "admin_url": (
                f"/admin/ledger/ledgersaga/{instance.pk}/change/"
            ),
        },
    )


@receiver(pre_save, sender=DepositSweepJob)
def deposit_sweep_job_before_save(sender, instance, **kwargs):
    _remember_previous_value(
        DepositSweepJob,
        instance,
        "status",
        "_notification_previous_status",
    )


@receiver(post_save, sender=DepositSweepJob)
def residual_deposit_review_requested(
    sender,
    instance,
    created=False,
    **kwargs,
):
    # Balance-verification residual observations can be coalesced into an
    # already-active sweep. Only a distinct residual sweep job needs review.
    metadata = (
        instance.metadata
        if isinstance(instance.metadata, dict)
        else {}
    )
    if not created or metadata.get("source") != "residual_deposit":
        return

    try:
        session = instance.deposit_session
        user = session.user
    except Exception:
        session = None
        user = None

    try:
        observed = instance.observed_transfer
    except Exception:
        observed = None

    raw_payload = (
        observed.raw_payload
        if observed is not None
        and isinstance(observed.raw_payload, dict)
        else {}
    )
    amount_display = _format_scaled_amount(instance.amount)
    if amount_display:
        amount_display = (
            f"{amount_display} {instance.asset_code}"
        )

    _queue_admin_notification(
        "deposit.residual_review_requested",
        {
            "severity": "review",
            "title": "Residual deposit needs review",
            "object_type": "observed_onchain_transfer",
            "object_id": getattr(observed, "pk", None),
            "event_key": getattr(observed, "event_key", ""),
            "chain": instance.chain,
            "asset_code": instance.asset_code,
            "amount": int(instance.amount),
            "amount_display": amount_display,
            "txid": getattr(observed, "txid", ""),
            "log_index": getattr(observed, "log_index", None),
            "confirmations": int(
                getattr(observed, "confirmations", 0) or 0
            ),
            "deposit_address": (
                getattr(session, "deposit_address", "")
                if session is not None
                else instance.source_address
            ),
            "deposit_session_id": (
                getattr(session, "pk", None)
                if session is not None
                else None
            ),
            "deposit_session_public_id": (
                str(getattr(session, "public_id", "") or "")
                if session is not None
                else ""
            ),
            "reason": str(
                raw_payload.get("residual_reason")
                or "post-finalized-session transfer"
            ),
            "user": _user_payload(user),
            "admin_url": (
                f"/admin/ledger/depositsession/{session.pk}/change/"
                if session is not None
                else ""
            ),
        },
    )


@receiver(post_save, sender=DepositSweepJob)
def deposit_sweep_failed(
    sender,
    instance,
    created=False,
    **kwargs,
):
    previous = getattr(
        instance,
        "_notification_previous_status",
        None,
    )
    if (
        instance.status != DepositSweepJob.STATUS_FAILED
        or (
            not created
            and previous == DepositSweepJob.STATUS_FAILED
        )
    ):
        return

    try:
        session = instance.deposit_session
        user = session.user
    except Exception:
        session = None
        user = None

    amount_display = _format_scaled_amount(instance.amount)
    if amount_display:
        amount_display = (
            f"{amount_display} {instance.asset_code}"
        )

    _queue_admin_notification(
        "deposit.sweep_failed",
        {
            "severity": "critical",
            "title": "Deposit sweep failed",
            "object_type": "deposit_sweep_job",
            "object_id": instance.pk,
            "public_id": str(instance.public_id),
            "chain": instance.chain,
            "asset_code": instance.asset_code,
            "amount": int(instance.amount),
            "amount_display": amount_display,
            "source_address": instance.source_address,
            "destination_address": instance.destination_address,
            "gas_funding_txid": instance.gas_funding_txid,
            "sweep_txid": instance.sweep_txid,
            "error_code": instance.last_error_code,
            "error": instance.last_error,
            "deposit_session_id": (
                getattr(session, "pk", None)
                if session is not None
                else None
            ),
            "deposit_session_public_id": (
                str(getattr(session, "public_id", "") or "")
                if session is not None
                else ""
            ),
            "user": _user_payload(user),
            "admin_url": (
                f"/admin/ledger/depositsession/{session.pk}/change/"
                if session is not None
                else ""
            ),
        },
    )


# P2P WebSocket status broadcasts
@receiver(post_save, sender=P2POrder)
def broadcast_p2p_order_status(sender, instance, **kwargs):
    from .p2p_realtime import publish_p2p_order_status

    public_id = str(instance.public_id)
    status = str(instance.status)
    can_send = bool(instance.chat_writable)
    transaction.on_commit(
        lambda: publish_p2p_order_status(
            public_id=public_id,
            status=status,
            can_send=can_send,
        )
    )
