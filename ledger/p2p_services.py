
from __future__ import annotations

import logging
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Count, F, Q
from django.utils import timezone

from .models import (
    LedgerTransaction,
    P2PAgentAssignment,
    P2PMakerProfile,
    P2PMessage,
    P2POrder,
    P2PReview,
    TokenPack,
    TokenWallet,
)
from .services import (
    _convert_platform_token_units_to_canonical_stable_units,
    apply_ledger_transaction,
    get_system_wallet,
    reverse_ledger_transaction,
)


logger = logging.getLogger(__name__)


P2P_PROVIDER_KEY = "p2p"
P2P_NO_AGENT_MESSAGE = "No P2P agent is currently available for this payment method."
P2P_CHECKOUT_METHODS = (
    P2POrder.PAYMENT_METHOD_CARD,
    P2POrder.PAYMENT_METHOD_APPLE_PAY,
    P2POrder.PAYMENT_METHOD_GOOGLE_PAY,
    P2POrder.PAYMENT_METHOD_PAYPAL,
    P2POrder.PAYMENT_METHOD_REVOLUT,
    P2POrder.PAYMENT_METHOD_BANK_TRANSFER,
)


class P2PNoAgentAvailable(ValidationError):
    pass


def _agent_response_timeout_seconds() -> int:
    return max(30, int(getattr(settings, "P2P_AGENT_RESPONSE_TIMEOUT_SECONDS", 300)))


def _trade_timeout_seconds() -> int:
    return max(300, int(getattr(settings, "P2P_TRADE_TIMEOUT_SECONDS", 86400)))


def _p2p_service_actor():
    username = str(getattr(settings, "P2P_INTERNAL_SERVICE_USERNAME", "p2p-service") or "").strip()
    if not username:
        raise ImproperlyConfigured("P2P_INTERNAL_SERVICE_USERNAME is not configured")
    actor = get_user_model().objects.filter(username=username, is_active=True).first()
    if actor is None:
        raise ImproperlyConfigured("Configured P2P internal service actor does not exist")
    return actor


def _p2p_settlement_wallet() -> TokenWallet:
    return get_system_wallet(TokenWallet.SYSTEM_P2P_SETTLEMENT, allow_negative=False)


def _method_filter(payment_method: str) -> Q:
    method = str(payment_method or "").strip().lower()
    if method in {
        P2POrder.PAYMENT_METHOD_CARD,
        P2POrder.PAYMENT_METHOD_APPLE_PAY,
        P2POrder.PAYMENT_METHOD_GOOGLE_PAY,
    }:
        return Q(card_enabled=True)
    if method == P2POrder.PAYMENT_METHOD_PAYPAL:
        return Q(paypal_enabled=True)
    if method == P2POrder.PAYMENT_METHOD_REVOLUT:
        return Q(revolut_enabled=True)
    if method == P2POrder.PAYMENT_METHOD_BANK_TRANSFER:
        return Q(bank_transfer_enabled=True) | Q(sepa_enabled=True) | Q(wise_enabled=True)
    if method == P2POrder.PAYMENT_METHOD_SEPA:
        return Q(sepa_enabled=True)
    if method == P2POrder.PAYMENT_METHOD_WISE:
        return Q(wise_enabled=True)
    return Q(pk__in=[])


def get_p2p_checkout_options() -> list[dict]:
    """Return P2P as a normal checkout provider for currently configured pools.

    The displayed provider price stays deliberately dynamic. The exact agent
    and exact Transaction value are frozen only when P2POrder is created.
    """
    base_qs = P2PMakerProfile.objects.filter(
        status=P2PMakerProfile.STATUS_ACTIVE,
        accepting_orders=True,
    )
    labels = {
        P2POrder.PAYMENT_METHOD_CARD: "Card",
        P2POrder.PAYMENT_METHOD_APPLE_PAY: "Apple Pay",
        P2POrder.PAYMENT_METHOD_GOOGLE_PAY: "Google Pay",
        P2POrder.PAYMENT_METHOD_PAYPAL: "PayPal",
        P2POrder.PAYMENT_METHOD_REVOLUT: "Revolut",
        P2POrder.PAYMENT_METHOD_BANK_TRANSFER: "Bank transfer",
    }
    options = []
    for method in P2P_CHECKOUT_METHODS:
        if not base_qs.filter(_method_filter(method)).exists():
            continue
        options.append(
            {
                "key": f"p2p:{method}",
                "provider_key": P2P_PROVIDER_KEY,
                "p2p_payment_method": method,
                "payment_method_key": f"p2p:{method}",
                "payment_method_label": "P2P agent",
                "payment_method_type": "provider",
                "payment_currency": "USD",
                "payment_currency_usd_rate": "1",
                "payment_requires_route_selection": False,
                "payment_open_new_tab": False,
                "payment_price_mode": "p2p_dynamic",
                "route_label": f"P2P · {labels[method]}",
                "min_amount": 0,
                "min_amount_display": "0",
                "chain": "",
                "asset_code": "",
            }
        )
    return options


def _active_order_statuses() -> tuple[str, ...]:
    return (
        P2POrder.STATUS_WAITING_AGENT,
        P2POrder.STATUS_CHAT_OPEN,
        P2POrder.STATUS_FIAT_SENT,
        P2POrder.STATUS_DISPUTED,
    )


def _select_maker_locked(*, buyer, payment_method: str, base_amount: int, order: P2POrder | None = None):
    excluded_ids = set()
    if order is not None:
        excluded_ids = set(order.assignments.values_list("maker_id", flat=True))

    candidates = (
        P2PMakerProfile.objects.filter(
            status=P2PMakerProfile.STATUS_ACTIVE,
            accepting_orders=True,
            min_order_amount__lte=int(base_amount),
        )
        .filter(Q(max_order_amount__isnull=True) | Q(max_order_amount__gte=int(base_amount)))
        .filter(_method_filter(payment_method))
        .exclude(user=buyer)
        .exclude(pk__in=excluded_ids)
        .annotate(
            active_order_count=Count(
                "orders",
                filter=Q(orders__status__in=_active_order_statuses()),
                distinct=True,
            )
        )
        .filter(active_order_count__lt=F("max_concurrent_orders"))
        .order_by("active_order_count", F("last_assigned_at").asc(nulls_first=True), "id")
    )

    for maker_id in list(candidates.values_list("id", flat=True)):
        maker = P2PMakerProfile.objects.select_for_update().get(pk=maker_id)
        current_load = P2POrder.objects.filter(
            maker=maker,
            status__in=_active_order_statuses(),
        ).count()
        if current_load < int(maker.max_concurrent_orders):
            return maker
    return None


def _price_for_maker(*, base_amount: int, maker: P2PMakerProfile) -> tuple[Decimal, int, int]:
    commission_percent = Decimal(maker.commission_percent or 0)
    commission_amount = int(
        (Decimal(int(base_amount)) * commission_percent / Decimal("100")).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    )
    return commission_percent, commission_amount, int(base_amount) + commission_amount


def _make_assignment(*, order: P2POrder, maker: P2PMakerProfile) -> P2PAgentAssignment:
    now = timezone.now()
    assignment = P2PAgentAssignment.objects.create(
        order=order,
        maker=maker,
        commission_percent_snapshot=order.commission_percent_snapshot,
        commission_amount_snapshot=order.commission_amount,
        transaction_amount_snapshot=order.platform_amount,
        offered_at=now,
        expires_at=now + timedelta(seconds=_agent_response_timeout_seconds()),
    )
    maker.last_assigned_at = now
    maker.save(update_fields=["last_assigned_at", "updated_at"])
    return assignment


def _queue_assignment(assignment: P2PAgentAssignment) -> None:
    assignment_id = int(assignment.id)
    timeout = max(1, int((assignment.expires_at - timezone.now()).total_seconds()))

    def _enqueue():
        try:
            from .p2p_tasks import expire_p2p_agent_offer, notify_p2p_agent_offer

            notify_p2p_agent_offer.delay(assignment_id)
            expire_p2p_agent_offer.apply_async(args=[assignment_id], countdown=timeout)
        except Exception:
            # The P2P order is already committed. A broker outage must not make
            # the checkout look rolled back or trigger a duplicate order retry.
            logger.exception("Failed to enqueue P2P assignment %s notification/expiry", assignment_id)

    transaction.on_commit(_enqueue)


def _queue_trade_timeout(order: P2POrder) -> None:
    if order.trade_expires_at is None:
        return
    order_id = int(order.id)
    timeout = max(1, int((order.trade_expires_at - timezone.now()).total_seconds()))

    def _enqueue():
        try:
            from .p2p_tasks import expire_p2p_trade

            expire_p2p_trade.apply_async(args=[order_id], countdown=timeout)
        except Exception:
            # The database status is authoritative; timeout checks can be
            # retried operationally without undoing an already-open trade.
            logger.exception("Failed to enqueue P2P trade %s timeout", order_id)

    transaction.on_commit(_enqueue)


@transaction.atomic
def create_p2p_order_for_checkout(*, buyer, token_pack: TokenPack, payment_method: str) -> P2POrder:
    method = str(payment_method or "").strip().lower()
    if method not in P2P_CHECKOUT_METHODS:
        raise ValidationError("Unsupported P2P payment method")

    base_amount = _convert_platform_token_units_to_canonical_stable_units(int(token_pack.token_amount))
    maker = _select_maker_locked(
        buyer=buyer,
        payment_method=method,
        base_amount=base_amount,
    )
    if maker is None:
        raise P2PNoAgentAvailable(P2P_NO_AGENT_MESSAGE)

    commission_percent, commission_amount, transaction_amount = _price_for_maker(
        base_amount=base_amount,
        maker=maker,
    )
    order = P2POrder.objects.create(
        buyer=buyer,
        maker=maker,
        token_pack=token_pack,
        token_amount=int(token_pack.token_amount),
        base_amount=int(base_amount),
        commission_percent_snapshot=commission_percent,
        commission_amount=commission_amount,
        platform_amount=transaction_amount,
        payment_method=method,
        status=P2POrder.STATUS_WAITING_AGENT,
    )
    assignment = _make_assignment(order=order, maker=maker)
    _queue_assignment(assignment)
    return order


@transaction.atomic
def find_new_p2p_agent(*, order_id: int, buyer_id: int) -> P2POrder:
    order = (
        P2POrder.objects.select_for_update()
        .select_related("buyer", "token_pack")
        .get(pk=order_id)
    )
    if order.buyer_id != buyer_id:
        raise PermissionDenied("Only the customer can find another P2P agent")
    if order.status != P2POrder.STATUS_WAITING_NEW_AGENT:
        raise ValidationError("A new P2P agent cannot be selected in the current state")

    maker = _select_maker_locked(
        buyer=order.buyer,
        payment_method=order.payment_method,
        base_amount=order.base_amount,
        order=order,
    )
    if maker is None:
        order.maker = None
        order.status = P2POrder.STATUS_NO_AGENT_AVAILABLE
        order.save(update_fields=["maker", "status", "updated_at"])
        return order

    commission_percent, commission_amount, transaction_amount = _price_for_maker(
        base_amount=order.base_amount,
        maker=maker,
    )
    order.maker = maker
    order.commission_percent_snapshot = commission_percent
    order.commission_amount = commission_amount
    order.platform_amount = transaction_amount
    order.status = P2POrder.STATUS_WAITING_AGENT
    order.save(
        update_fields=[
            "maker",
            "commission_percent_snapshot",
            "commission_amount",
            "platform_amount",
            "status",
            "updated_at",
        ]
    )
    assignment = _make_assignment(order=order, maker=maker)
    _queue_assignment(assignment)
    return order


@transaction.atomic
def cancel_p2p_order(*, order_id: int, buyer_id: int) -> P2POrder:
    order = P2POrder.objects.select_for_update().get(pk=order_id)
    if order.buyer_id != buyer_id:
        raise PermissionDenied("Only the customer can cancel this P2P order")
    if order.status not in {
        P2POrder.STATUS_WAITING_AGENT,
        P2POrder.STATUS_WAITING_NEW_AGENT,
        P2POrder.STATUS_NO_AGENT_AVAILABLE,
    }:
        raise ValidationError("This P2P order can no longer be canceled")
    order.status = P2POrder.STATUS_CANCELED
    order.save(update_fields=["status", "updated_at"])
    return order


def _external_identity_matches(*, assignment: P2PAgentAssignment, channel: str, external_user_id) -> bool:
    external = str(external_user_id or "").strip()
    if channel == "telegram":
        expected = str(assignment.maker.telegram_user_id or "").strip()
    elif channel == "discord":
        expected = str(assignment.maker.discord_user_id or "").strip()
    else:
        return False
    return bool(expected and external and expected == external)


def _expire_assignment_locked(assignment: P2PAgentAssignment, now) -> P2POrder:
    order = P2POrder.objects.select_for_update().get(pk=assignment.order_id)
    if assignment.status != P2PAgentAssignment.STATUS_OFFERED:
        return order
    assignment.status = P2PAgentAssignment.STATUS_EXPIRED
    assignment.responded_at = now
    assignment.save(update_fields=["status", "responded_at"])
    if order.status == P2POrder.STATUS_WAITING_AGENT and order.maker_id == assignment.maker_id:
        order.maker = None
        order.status = P2POrder.STATUS_WAITING_NEW_AGENT
        order.save(update_fields=["maker", "status", "updated_at"])
    return order


@transaction.atomic
def expire_p2p_agent_assignment(*, assignment_id: int) -> tuple[str, P2POrder | None, int | None]:
    assignment = (
        P2PAgentAssignment.objects.select_for_update()
        .select_related("maker", "order")
        .filter(pk=assignment_id)
        .first()
    )
    if assignment is None:
        return "missing", None, None
    if assignment.status != P2PAgentAssignment.STATUS_OFFERED:
        return assignment.status, assignment.order, None
    now = timezone.now()
    if assignment.expires_at > now:
        remaining = max(1, int((assignment.expires_at - now).total_seconds()))
        return "not_yet", assignment.order, remaining
    order = _expire_assignment_locked(assignment, now)
    return "expired", order, None


@transaction.atomic
def respond_to_p2p_agent_offer(*, action_token, action: str, channel: str, external_user_id) -> tuple[str, P2POrder]:
    assignment = (
        P2PAgentAssignment.objects.select_for_update()
        .select_related("maker__user", "order__buyer")
        .get(action_token=action_token)
    )
    if not _external_identity_matches(
        assignment=assignment,
        channel=channel,
        external_user_id=external_user_id,
    ):
        raise PermissionDenied("This external account is not linked to the selected P2P agent")

    order = P2POrder.objects.select_for_update().get(pk=assignment.order_id)
    if assignment.status != P2PAgentAssignment.STATUS_OFFERED:
        return assignment.status, order
    now = timezone.now()
    if assignment.expires_at <= now:
        order = _expire_assignment_locked(assignment, now)
        return "expired", order
    if order.status != P2POrder.STATUS_WAITING_AGENT or order.maker_id != assignment.maker_id:
        return "superseded", order

    normalized_action = str(action or "").strip().lower()
    if normalized_action == "decline":
        assignment.status = P2PAgentAssignment.STATUS_DECLINED
        assignment.responded_at = now
        assignment.save(update_fields=["status", "responded_at"])
        order.maker = None
        order.status = P2POrder.STATUS_WAITING_NEW_AGENT
        order.save(update_fields=["maker", "status", "updated_at"])
        return "declined", order
    if normalized_action != "accept":
        raise ValidationError("Unsupported P2P agent action")

    maker_wallet, _ = TokenWallet.objects.get_or_create(
        user=assignment.maker.user,
        defaults={"wallet_type": TokenWallet.TYPE_USER, "allow_negative": False},
    )
    settlement_wallet = _p2p_settlement_wallet()
    service_actor = _p2p_service_actor()
    funding_txn = apply_ledger_transaction(
        actor=service_actor,
        kind="p2p_withdrawal_funding",
        entries=[
            (maker_wallet, -int(order.token_amount)),
            (settlement_wallet, int(order.token_amount)),
        ],
        created_by=service_actor,
        external_id=f"p2p_agent_funding:{order.public_id}",
        memo=f"P2P agent funding for order {order.public_id}",
        metadata={
            "p2p_order_id": order.id,
            "p2p_order_public_id": str(order.public_id),
            "p2p_assignment_id": assignment.id,
            "p2p_agent_user_id": assignment.maker.user_id,
            "token_amount": int(order.token_amount),
            "transaction_amount": int(order.platform_amount),
            "commission_amount": int(order.commission_amount),
        },
    )

    assignment.status = P2PAgentAssignment.STATUS_ACCEPTED
    assignment.responded_at = now
    assignment.save(update_fields=["status", "responded_at"])

    order.funding_txn = funding_txn
    order.funded_at = now
    order.accepted_at = now
    order.trade_expires_at = now + timedelta(seconds=_trade_timeout_seconds())
    order.status = P2POrder.STATUS_CHAT_OPEN
    order.save(
        update_fields=[
            "funding_txn",
            "funded_at",
            "accepted_at",
            "trade_expires_at",
            "status",
            "updated_at",
        ]
    )
    P2PMessage.objects.create(
        order=order,
        sender=None,
        kind=P2PMessage.KIND_SYSTEM,
        body="The P2P agent accepted and funded the transaction. The private exchange is now open.",
    )
    _queue_trade_timeout(order)
    return "accepted", order


@transaction.atomic
def mark_p2p_fiat_sent(*, order_id: int, user_id: int) -> P2POrder:
    order = P2POrder.objects.select_for_update().get(pk=order_id)
    if order.buyer_id != user_id:
        raise PermissionDenied("Only the customer can mark the money as sent")
    if order.status != P2POrder.STATUS_CHAT_OPEN:
        raise ValidationError("Money can only be marked sent once the exchange is open")
    now = timezone.now()
    order.status = P2POrder.STATUS_FIAT_SENT
    order.buyer_marked_paid_at = now
    order.trade_expires_at = now + timedelta(seconds=_trade_timeout_seconds())
    order.save(
        update_fields=["status", "buyer_marked_paid_at", "trade_expires_at", "updated_at"]
    )
    P2PMessage.objects.create(
        order=order,
        sender=None,
        kind=P2PMessage.KIND_SYSTEM,
        body="Customer marked the money as sent.",
    )
    _queue_trade_timeout(order)
    return order


def _settle_to_customer_locked(*, order: P2POrder, created_by) -> LedgerTransaction:
    if order.settlement_txn_id:
        return order.settlement_txn
    settlement_wallet = _p2p_settlement_wallet()
    buyer_wallet, _ = TokenWallet.objects.get_or_create(
        user=order.buyer,
        defaults={"wallet_type": TokenWallet.TYPE_USER, "allow_negative": False},
    )
    service_actor = _p2p_service_actor()
    return apply_ledger_transaction(
        actor=service_actor,
        kind="p2p_customer_settlement",
        entries=[
            (settlement_wallet, -int(order.token_amount)),
            (buyer_wallet, int(order.token_amount)),
        ],
        created_by=service_actor,
        external_id=f"p2p_customer_settlement:{order.public_id}",
        memo=f"P2P settlement to customer for order {order.public_id}",
        metadata={
            "p2p_order_id": order.id,
            "p2p_order_public_id": str(order.public_id),
            "token_amount": int(order.token_amount),
            "initiated_by_user_id": getattr(created_by, "id", None),
        },
    )


def _rolling_average(old_value, old_count: int, new_value: int) -> int:
    if old_value in (None, "") or old_count <= 0:
        return max(0, int(new_value))
    return max(0, int(round(((int(old_value) * old_count) + int(new_value)) / (old_count + 1))))


def _record_normal_completion(*, order: P2POrder, completed_at) -> None:
    if order.maker_id is None:
        return
    maker = P2PMakerProfile.objects.select_for_update().get(pk=order.maker_id)
    old_count = int(maker.completed_orders)
    if order.accepted_at is not None:
        completion_seconds = max(0, int((completed_at - order.accepted_at).total_seconds()))
        maker.avg_completion_time_seconds = _rolling_average(
            maker.avg_completion_time_seconds, old_count, completion_seconds
        )
    assignment = order.assignments.filter(status=P2PAgentAssignment.STATUS_ACCEPTED).order_by("-responded_at").first()
    if assignment and assignment.responded_at:
        response_seconds = max(0, int((assignment.responded_at - assignment.offered_at).total_seconds()))
        maker.avg_response_time_seconds = _rolling_average(
            maker.avg_response_time_seconds, old_count, response_seconds
        )
    maker.completed_orders = old_count + 1
    maker.total_volume = int(maker.total_volume) + int(order.platform_amount)
    maker.save(
        update_fields=[
            "completed_orders",
            "total_volume",
            "avg_response_time_seconds",
            "avg_completion_time_seconds",
            "updated_at",
        ]
    )


@transaction.atomic
def mark_p2p_fiat_received(*, order_id: int, user_id: int) -> P2POrder:
    order = (
        P2POrder.objects.select_for_update()
        .select_related("buyer", "maker__user", "settlement_txn")
        .get(pk=order_id)
    )
    if order.maker_id is None or order.maker.user_id != user_id:
        raise PermissionDenied("Only the assigned P2P agent can confirm receipt")
    if order.status != P2POrder.STATUS_FIAT_SENT:
        raise ValidationError("The customer must mark the money as sent first")

    settlement_txn = _settle_to_customer_locked(order=order, created_by=order.maker.user)
    now = timezone.now()
    order.settlement_txn = settlement_txn
    order.status = P2POrder.STATUS_COMPLETED
    order.completed_at = now
    order.trade_expires_at = None
    order.save(
        update_fields=["settlement_txn", "status", "completed_at", "trade_expires_at", "updated_at"]
    )
    P2PMessage.objects.create(
        order=order,
        sender=None,
        kind=P2PMessage.KIND_SYSTEM,
        body="P2P agent confirmed receipt. The purchased tokens were credited to the customer.",
    )
    _record_normal_completion(order=order, completed_at=now)
    return order


@transaction.atomic
def enter_p2p_dispute(*, order_id: int, reason: str = "") -> P2POrder:
    order = P2POrder.objects.select_for_update().select_related("maker").get(pk=order_id)
    if order.status == P2POrder.STATUS_DISPUTED:
        return order
    if order.status not in {P2POrder.STATUS_CHAT_OPEN, P2POrder.STATUS_FIAT_SENT}:
        raise ValidationError("Only an active P2P exchange can enter dispute")
    now = timezone.now()
    order.status = P2POrder.STATUS_DISPUTED
    order.disputed_at = now
    order.trade_expires_at = None
    order.save(update_fields=["status", "disputed_at", "trade_expires_at", "updated_at"])
    if order.maker_id:
        P2PMakerProfile.objects.filter(pk=order.maker_id).update(disputed_orders=F("disputed_orders") + 1)
    P2PMessage.objects.create(
        order=order,
        sender=None,
        kind=P2PMessage.KIND_SYSTEM,
        body=("The transaction entered dispute." + (f" {reason}" if reason else ""))[:4000],
    )
    return order


@transaction.atomic
def expire_p2p_trade_if_due(*, order_id: int) -> tuple[str, P2POrder | None, int | None]:
    order = P2POrder.objects.select_for_update().filter(pk=order_id).first()
    if order is None:
        return "missing", None, None
    if order.status not in {P2POrder.STATUS_CHAT_OPEN, P2POrder.STATUS_FIAT_SENT}:
        return order.status, order, None
    now = timezone.now()
    if order.trade_expires_at is None:
        return "no_expiry", order, None
    if order.trade_expires_at > now:
        remaining = max(1, int((order.trade_expires_at - now).total_seconds()))
        return "not_yet", order, remaining
    # Inline the transition while holding the same row lock.
    order.status = P2POrder.STATUS_DISPUTED
    order.disputed_at = now
    order.trade_expires_at = None
    order.save(update_fields=["status", "disputed_at", "trade_expires_at", "updated_at"])
    if order.maker_id:
        P2PMakerProfile.objects.filter(pk=order.maker_id).update(disputed_orders=F("disputed_orders") + 1)
    P2PMessage.objects.create(
        order=order,
        sender=None,
        kind=P2PMessage.KIND_SYSTEM,
        body="The transaction timed out and entered dispute.",
    )
    return "disputed", order, None


@transaction.atomic
def resolve_p2p_dispute(*, order_id: int, winner: str, resolved_by) -> P2POrder:
    if not getattr(resolved_by, "is_staff", False) and not getattr(resolved_by, "is_superuser", False):
        raise PermissionDenied("Staff access required")
    order = (
        P2POrder.objects.select_for_update()
        .select_related("buyer", "maker__user", "funding_txn", "settlement_txn")
        .get(pk=order_id)
    )
    if order.status != P2POrder.STATUS_DISPUTED:
        raise ValidationError("Only disputed P2P orders can be resolved")
    if not order.funding_txn_id:
        raise ValidationError("Disputed order is missing the agent funding transaction")

    now = timezone.now()
    normalized = str(winner or "").strip().lower()
    if normalized == "customer":
        settlement_txn = _settle_to_customer_locked(order=order, created_by=resolved_by)
        order.settlement_txn = settlement_txn
        order.status = P2POrder.STATUS_COMPLETED
        order.completed_at = now
        order.dispute_resolution = P2POrder.DISPUTE_CUSTOMER_WINS
        message = "Dispute resolved in favor of the customer. The purchased tokens were credited."
    elif normalized == "agent":
        service_actor = _p2p_service_actor()
        reversal = reverse_ledger_transaction(
            actor=service_actor,
            original_txn=order.funding_txn,
            created_by=service_actor,
            external_id=f"p2p_agent_refund:{order.public_id}",
            memo=f"P2P disputed funding returned to agent for order {order.public_id}",
            metadata={
                "p2p_order_id": order.id,
                "p2p_order_public_id": str(order.public_id),
                "resolved_by_user_id": resolved_by.id,
            },
            reversal_kind="p2p_withdrawal_funding_reversal",
        )
        order.settlement_txn = reversal
        order.status = P2POrder.STATUS_CANCELED
        order.dispute_resolution = P2POrder.DISPUTE_AGENT_WINS
        message = "Dispute resolved in favor of the P2P agent. The funded tokens were returned."
    else:
        raise ValidationError("winner must be 'customer' or 'agent'")

    order.resolved_by = resolved_by
    order.resolved_at = now
    order.save(
        update_fields=[
            "settlement_txn",
            "status",
            "completed_at",
            "dispute_resolution",
            "resolved_by",
            "resolved_at",
            "updated_at",
        ]
    )
    P2PMessage.objects.create(order=order, sender=None, kind=P2PMessage.KIND_SYSTEM, body=message)
    return order


def _normalize_review_value(value, label: str) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{label} must be between 1 and 5") from exc
    if normalized < 1 or normalized > 5:
        raise ValidationError(f"{label} must be between 1 and 5")
    return normalized


@transaction.atomic
def submit_p2p_review(*, order_id: int, reviewer_id: int, ratings: dict) -> P2PReview:
    order = P2POrder.objects.select_for_update().select_related("maker__user").get(pk=order_id)
    if order.status != P2POrder.STATUS_COMPLETED:
        raise ValidationError("Reviews are only available after a completed transaction")
    if order.maker_id is None:
        raise ValidationError("Completed order has no P2P agent")
    participants = {order.buyer_id, order.maker.user_id}
    if reviewer_id not in participants:
        raise PermissionDenied("Only trade participants can review this transaction")
    reviewee_id = order.maker.user_id if reviewer_id == order.buyer_id else order.buyer_id
    if P2PReview.objects.filter(order=order, reviewer_id=reviewer_id).exists():
        raise ValidationError("You already reviewed this transaction")

    values = {
        key: _normalize_review_value(ratings.get(key), key.replace("_", " ").title())
        for key in (
            "communication",
            "responsiveness",
            "reliability",
            "payment_experience",
            "cooperation",
        )
    }
    review = P2PReview.objects.create(
        order=order,
        reviewer_id=reviewer_id,
        reviewee_id=reviewee_id,
        **values,
    )

    if reviewee_id == order.maker.user_id:
        received = list(P2PReview.objects.filter(reviewee_id=reviewee_id).only(
            "communication", "responsiveness", "reliability", "payment_experience", "cooperation"
        ))
        if received:
            average = Decimal(str(sum(item.score for item in received) / len(received))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            P2PMakerProfile.objects.filter(pk=order.maker_id).update(
                rating=average,
                rating_count=len(received),
            )
    return review
