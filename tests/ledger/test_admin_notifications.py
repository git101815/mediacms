from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from django.test import override_settings

import ledger.signals as signals
import ledger.tasks as tasks
from ledger.models import (
    DepositSweepJob,
    LedgerOutbox,
    LedgerSaga,
    WalletRequest,
)


def _user(pk=7):
    return SimpleNamespace(
        pk=pk,
        username="operator-target",
        email="target@example.com",
    )


def _capture_notifications(monkeypatch):
    queued = []
    monkeypatch.setattr(
        signals,
        "_queue_admin_notification",
        lambda event, payload: queued.append((event, payload)),
    )
    return queued


def test_admin_notification_task_is_disabled_during_testing(
    monkeypatch,
):
    post = Mock()
    monkeypatch.setattr(tasks.requests, "post", post)
    monkeypatch.setenv(
        "NOTIFICATION_WEBHOOK_URL",
        "https://n8n.example/webhook/notifications",
    )

    assert tasks.notify_admin_event(
        "ledger.saga_failed",
        {"object_id": 1},
        "event-test",
    ) is False
    post.assert_not_called()


@override_settings(TESTING=False)
def test_admin_notification_task_posts_generic_payload(
    monkeypatch,
):
    response = Mock()
    response.raise_for_status.return_value = None
    post = Mock(return_value=response)
    monkeypatch.setattr(tasks.requests, "post", post)
    monkeypatch.setenv(
        "NOTIFICATION_WEBHOOK_URL",
        "https://n8n.example/webhook/notifications",
    )
    monkeypatch.setenv(
        "NOTIFICATION_WEBHOOK_SECRET",
        "secret",
    )

    assert tasks.notify_admin_event(
        "ledger.saga_failed",
        {"object_id": 42, "severity": "critical"},
        "event-42",
    ) is True

    kwargs = post.call_args.kwargs
    assert kwargs["json"]["event"] == "ledger.saga_failed"
    assert kwargs["json"]["event_id"] == "event-42"
    assert kwargs["json"]["object_id"] == 42
    assert (
        kwargs["headers"]["X-Notification-Secret"]
        == "secret"
    )
    assert (
        kwargs["headers"]["X-Notification-Event"]
        == "event-42"
    )
    assert kwargs["timeout"] == 5.0


def test_wallet_review_notification_only_on_pending_transition(
    monkeypatch,
):
    queued = _capture_notifications(monkeypatch)
    wallet = SimpleNamespace(
        user=_user(),
    )
    instance = SimpleNamespace(
        pk=11,
        status=WalletRequest.STATUS_PENDING,
        request_type=WalletRequest.REQUEST_TYPE_WITHDRAWAL,
        amount=1_500_000,
        asset_code="TOKENS",
        destination_address="0xabc",
        notes="withdraw",
        reference="wd_test",
        wallet_id=3,
        wallet=wallet,
    )

    signals.wallet_request_review_requested(
        sender=WalletRequest,
        instance=instance,
        created=True,
    )

    assert len(queued) == 1
    event, payload = queued[0]
    assert event == "wallet.review_requested"
    assert payload["amount_display"] == "1.5 TOKENS"
    assert payload["destination"] == "0xabc"
    assert payload["user"]["id"] == 7

    instance._notification_previous_status = (
        WalletRequest.STATUS_PENDING
    )
    signals.wallet_request_review_requested(
        sender=WalletRequest,
        instance=instance,
        created=False,
    )
    assert len(queued) == 1


def test_residual_deposit_notification_only_for_new_residual_sweep_job(
    monkeypatch,
):
    queued = _capture_notifications(monkeypatch)
    session = SimpleNamespace(
        pk=21,
        public_id="session-public",
        user=_user(),
        deposit_address="0xdeposit",
    )
    observed = SimpleNamespace(
        pk=22,
        event_key="event-key",
        txid="0xtx",
        log_index=1,
        confirmations=12,
        raw_payload={
            "ledger_residual_deposit": True,
            "residual_reason": "post_finalized_session_transfer",
        },
    )
    instance = SimpleNamespace(
        pk=23,
        public_id="sweep-public",
        chain="ethereum",
        asset_code="USDT",
        amount=2_500_000,
        source_address="0xdeposit",
        metadata={"source": "residual_deposit"},
        observed_transfer=observed,
        deposit_session=session,
    )

    signals.residual_deposit_review_requested(
        sender=DepositSweepJob,
        instance=instance,
        created=True,
    )

    assert len(queued) == 1
    event, payload = queued[0]
    assert event == "deposit.residual_review_requested"
    assert payload["amount_display"] == "2.5 USDT"
    assert payload["deposit_session_id"] == 21
    assert payload["object_id"] == 22
    assert payload["event_key"] == "event-key"
    assert payload["txid"] == "0xtx"
    assert payload["reason"] == "post_finalized_session_transfer"

    signals.residual_deposit_review_requested(
        sender=DepositSweepJob,
        instance=instance,
        created=False,
    )
    assert len(queued) == 1

    instance.metadata = {
        "source": "credited_deposit",
        "has_coalesced_residual_balance_observations": True,
    }
    signals.residual_deposit_review_requested(
        sender=DepositSweepJob,
        instance=instance,
        created=True,
    )
    assert len(queued) == 1


@pytest.mark.parametrize(
    (
        "handler",
        "model",
        "status",
        "event_name",
        "instance",
    ),
    [
        (
            signals.ledger_outbox_dead_lettered,
            LedgerOutbox,
            LedgerOutbox.STATUS_DEAD_LETTERED,
            "ledger.outbox_dead_lettered",
            SimpleNamespace(
                pk=31,
                status=LedgerOutbox.STATUS_DEAD_LETTERED,
                topic="wallet.updated",
                aggregate_type="ledger_transaction",
                aggregate_id=9,
                txn_id=8,
                txn=SimpleNamespace(external_id="txn-ext"),
                fail_count=5,
                dead_letter_reason="delivery failed",
                last_error="delivery failed",
            ),
        ),
        (
            signals.ledger_saga_failed,
            LedgerSaga,
            LedgerSaga.STATUS_FAILED,
            "ledger.saga_failed",
            SimpleNamespace(
                pk=41,
                status=LedgerSaga.STATUS_FAILED,
                saga_type="withdrawal",
                external_id="saga-ext",
                last_error="step failed",
                created_by=_user(),
            ),
        ),
        (
            signals.deposit_sweep_failed,
            DepositSweepJob,
            DepositSweepJob.STATUS_FAILED,
            "deposit.sweep_failed",
            SimpleNamespace(
                pk=51,
                public_id="sweep-public",
                status=DepositSweepJob.STATUS_FAILED,
                chain="ethereum",
                asset_code="USDT",
                amount=3_000_000,
                source_address="0xsource",
                destination_address="0xdest",
                gas_funding_txid="0xgas",
                sweep_txid="0xsweep",
                last_error_code="",
                last_error="sweep failed",
                deposit_session=SimpleNamespace(
                    pk=52,
                    public_id="deposit-public",
                    user=_user(),
                ),
            ),
        ),
    ],
)
def test_failure_notifications_only_fire_on_transition(
    monkeypatch,
    handler,
    model,
    status,
    event_name,
    instance,
):
    queued = _capture_notifications(monkeypatch)
    instance._notification_previous_status = "before"

    handler(
        sender=model,
        instance=instance,
        created=False,
    )

    assert len(queued) == 1
    assert queued[0][0] == event_name

    instance._notification_previous_status = status
    handler(
        sender=model,
        instance=instance,
        created=False,
    )
    assert len(queued) == 1
