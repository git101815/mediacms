from __future__ import annotations

import secrets
import uuid
from datetime import timezone as dt_timezone
from decimal import Decimal, InvalidOperation

from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Exists, OuterRef
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from .models import DepositSession, DepositSweepJob, OrphanDepositRecoveryAudit
from .services import (
    LEDGER_OPERATION_FLAG_SWEEP_CLAIM,
    _get_route_onchain_decimals,
    _normalize_chain,
    _normalize_evm_address,
    _parse_derivation_index_from_ref,
    _require_perm,
    require_ledger_operation_enabled,
)


ACTIVE_DEPOSIT_SESSION_STATUSES = {
    DepositSession.STATUS_AWAITING_PAYMENT,
    DepositSession.STATUS_SEEN_ONCHAIN,
    DepositSession.STATUS_CONFIRMING,
}
ACTIVE_SWEEP_JOB_STATUSES = {
    DepositSweepJob.STATUS_PENDING,
    DepositSweepJob.STATUS_READY_TO_SWEEP,
    DepositSweepJob.STATUS_FUNDING_BROADCASTED,
    DepositSweepJob.STATUS_SWEEP_BROADCASTED,
}
TERMINAL_AUDIT_STATUSES = {
    OrphanDepositRecoveryAudit.STATUS_EMPTY_FINAL,
    OrphanDepositRecoveryAudit.STATUS_DUST_FINAL,
    OrphanDepositRecoveryAudit.STATUS_IGNORED_FINAL,
    OrphanDepositRecoveryAudit.STATUS_SWEPT_TOKEN_FINAL,
    OrphanDepositRecoveryAudit.STATUS_SWEPT_NATIVE_FINAL,
    OrphanDepositRecoveryAudit.STATUS_SWEPT_BOTH_FINAL,
}
ALLOWED_RESULT_STATUSES = TERMINAL_AUDIT_STATUSES | {
    OrphanDepositRecoveryAudit.STATUS_PENDING_CHECK,
    OrphanDepositRecoveryAudit.STATUS_RETRYABLE_ERROR,
}
SUPPORTED_AUTOMATIC_ASSETS = {"USDT", "USDC"}
CLAIM_METADATA_KEY = "_sweeper_orphan_recovery_claim"


def _normalize_option_rows(option_rows) -> list[tuple[str, str, str]]:
    if not isinstance(option_rows, list) or not option_rows:
        raise ValidationError("Options payload must be a non-empty list")

    normalized = []
    seen = set()
    for row in option_rows:
        if not isinstance(row, dict):
            raise ValidationError("Each option row must be an object")
        chain = _normalize_chain(row.get("chain", ""))
        asset_code = str(row.get("asset_code", "") or "").strip().upper()
        token_contract = _normalize_evm_address(row.get("token_contract_address", ""))
        if not chain or not asset_code:
            raise ValidationError("Each option requires chain and asset_code")

        # Automatic orphan recovery is deliberately limited to stablecoin ERC20
        # deposit routes. Native-only sweep options are not deposit candidates.
        if asset_code not in SUPPORTED_AUTOMATIC_ASSETS or not token_contract:
            continue

        key = (chain, asset_code, token_contract)
        if key not in seen:
            seen.add(key)
            normalized.append(key)

    if not normalized:
        raise ValidationError("No supported stablecoin orphan-recovery options were supplied")
    return normalized


def _claim_expiry(metadata: dict):
    claim = metadata.get(CLAIM_METADATA_KEY)
    if not isinstance(claim, dict):
        return None
    expires_at = parse_datetime(str(claim.get("expires_at") or ""))
    if expires_at is None:
        return None
    if timezone.is_naive(expires_at):
        expires_at = timezone.make_aware(expires_at, timezone=dt_timezone.utc)
    return expires_at


def _claim_is_live(metadata: dict, *, now) -> bool:
    expires_at = _claim_expiry(metadata)
    return expires_at is not None and expires_at > now


def _candidate_queryset(*, chain: str, asset_code: str, token_contract: str, older_than_hours: int):
    cutoff = timezone.now() - timezone.timedelta(hours=int(older_than_hours))
    qs = (
        DepositSession.objects.filter(
            chain=chain,
            asset_code=asset_code,
            token_contract_address=token_contract,
            updated_at__lte=cutoff,
        )
        .exclude(status__in=ACTIVE_DEPOSIT_SESSION_STATUSES)
    )

    active_sweep_jobs = DepositSweepJob.objects.filter(
        deposit_session_id=OuterRef("pk"),
        status__in=ACTIVE_SWEEP_JOB_STATUSES,
    )
    terminal_audit = OrphanDepositRecoveryAudit.objects.filter(
        chain=OuterRef("chain"),
        deposit_address=OuterRef("deposit_address"),
        status__in=TERMINAL_AUDIT_STATUSES,
    )
    return (
        qs.annotate(has_active_sweep_job=Exists(active_sweep_jobs))
        .filter(has_active_sweep_job=False)
        .annotate(has_terminal_audit=Exists(terminal_audit))
        .filter(has_terminal_audit=False)
        .order_by("updated_at", "id")
    )


def _locked_audit_for_session(session: DepositSession) -> OrphanDepositRecoveryAudit:
    chain = _normalize_chain(session.chain)
    address = _normalize_evm_address(session.deposit_address)
    audit = (
        OrphanDepositRecoveryAudit.objects.select_for_update()
        .filter(chain=chain, deposit_address=address)
        .first()
    )
    if audit is None:
        try:
            with transaction.atomic():
                audit = OrphanDepositRecoveryAudit.objects.create(
                    deposit_session=session,
                    chain=chain,
                    asset_code=str(session.asset_code or "").strip().upper(),
                    token_contract_address=_normalize_evm_address(session.token_contract_address),
                    deposit_address=address,
                    address_derivation_ref=session.address_derivation_ref or "",
                    derivation_index=session.derivation_index,
                )
        except IntegrityError:
            audit = OrphanDepositRecoveryAudit.objects.select_for_update().get(
                chain=chain,
                deposit_address=address,
            )
    return audit


@transaction.atomic
def claim_orphan_recovery_candidates(
    *,
    actor,
    service_name: str,
    option_rows,
    limit: int,
    older_than_hours: int,
    lease_seconds: int,
):
    _require_perm(actor, "ledger.can_manage_deposit_sweep_jobs")
    require_ledger_operation_enabled(LEDGER_OPERATION_FLAG_SWEEP_CLAIM)

    normalized_options = _normalize_option_rows(option_rows)
    limit = int(limit)
    older_than_hours = int(older_than_hours)
    lease_seconds = int(lease_seconds)
    if limit <= 0:
        raise ValidationError("Claim limit must be positive")
    if older_than_hours < 0:
        raise ValidationError("older_than_hours must be >= 0")
    if lease_seconds <= 0:
        raise ValidationError("Claim lease must be positive")

    now = timezone.now()
    claim_until = now + timezone.timedelta(seconds=lease_seconds)
    claimed = []
    claimed_session_ids = set()

    for chain, asset_code, token_contract in normalized_options:
        qs = _candidate_queryset(
            chain=chain,
            asset_code=asset_code,
            token_contract=token_contract,
            older_than_hours=older_than_hours,
        )
        for session in qs.select_for_update(skip_locked=True)[: max(limit * 4, limit)]:
            if session.id in claimed_session_ids:
                continue

            audit = _locked_audit_for_session(session)
            metadata = dict(audit.metadata or {})
            if _claim_is_live(metadata, now=now):
                continue

            claim_token = uuid.uuid4().hex
            metadata[CLAIM_METADATA_KEY] = {
                "service": str(service_name),
                "token": claim_token,
                "expires_at": claim_until.isoformat(),
            }
            metadata["session_public_id"] = str(session.public_id)

            derivation_index = session.derivation_index
            if derivation_index is None:
                derivation_index = _parse_derivation_index_from_ref(session.address_derivation_ref)

            audit.deposit_session = session
            audit.asset_code = asset_code
            audit.token_contract_address = token_contract
            audit.address_derivation_ref = session.address_derivation_ref or ""
            audit.derivation_index = derivation_index
            audit.status = OrphanDepositRecoveryAudit.STATUS_PENDING_CHECK
            audit.finalized_at = None
            audit.metadata = metadata
            audit.save(
                update_fields=[
                    "deposit_session",
                    "asset_code",
                    "token_contract_address",
                    "address_derivation_ref",
                    "derivation_index",
                    "status",
                    "finalized_at",
                    "metadata",
                    "updated_at",
                ]
            )

            claimed.append(
                {
                    "session_public_id": str(session.public_id),
                    "claim_token": claim_token,
                    "claim_expires_at": claim_until.isoformat(),
                    "chain": chain,
                    "asset_code": asset_code,
                    "token_contract_address": token_contract,
                    "source_address": _normalize_evm_address(session.deposit_address),
                    "address_derivation_ref": session.address_derivation_ref or "",
                    "derivation_index": derivation_index,
                    "onchain_decimals": int(
                        _get_route_onchain_decimals(chain=chain, asset_code=asset_code)
                    ),
                    "existing_audit": {
                        "status": audit.status,
                        "funding_txid": audit.funding_txid,
                        "token_sweep_txid": audit.token_sweep_txid,
                        "native_sweep_txid": audit.native_sweep_txid,
                    },
                }
            )
            claimed_session_ids.add(session.id)
            if len(claimed) >= limit:
                return claimed

    return claimed


def _decimal_or_none(value, *, field_name: str):
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be a decimal or null") from exc
    if not parsed.is_finite():
        raise ValidationError(f"{field_name} must be finite")
    return parsed


@transaction.atomic
def record_orphan_recovery_result(
    *,
    actor,
    service_name: str,
    session_public_id,
    claim_token: str,
    payload: dict,
):
    _require_perm(actor, "ledger.can_manage_deposit_sweep_jobs")
    if not isinstance(payload, dict):
        raise ValidationError("Result payload must be an object")

    session = DepositSession.objects.filter(public_id=session_public_id).first()
    if session is None:
        raise DepositSession.DoesNotExist

    audit = (
        OrphanDepositRecoveryAudit.objects.select_for_update()
        .filter(
            chain=_normalize_chain(session.chain),
            deposit_address=_normalize_evm_address(session.deposit_address),
        )
        .first()
    )
    if audit is None:
        raise OrphanDepositRecoveryAudit.DoesNotExist

    metadata = dict(audit.metadata or {})
    claim = metadata.get(CLAIM_METADATA_KEY)
    if not isinstance(claim, dict):
        raise ValidationError("Orphan recovery candidate is not currently claimed")
    if str(claim.get("service") or "") != str(service_name):
        raise ValidationError("Orphan recovery claim belongs to another service")
    expected_token = str(claim.get("token") or "")
    supplied_token = str(claim_token or "")
    if not expected_token or not secrets.compare_digest(expected_token, supplied_token):
        raise ValidationError("Invalid orphan recovery claim token")

    status = str(payload.get("status") or "").strip()
    if status not in ALLOWED_RESULT_STATUSES:
        raise ValidationError("Invalid orphan recovery status")

    worker_metadata = payload.get("metadata") or {}
    if not isinstance(worker_metadata, dict):
        raise ValidationError("metadata must be an object")
    if status != OrphanDepositRecoveryAudit.STATUS_PENDING_CHECK:
        metadata.pop(CLAIM_METADATA_KEY, None)
    metadata.update(worker_metadata)
    metadata["session_public_id"] = str(session.public_id)

    audit.deposit_session = session
    audit.status = status
    audit.decision_reason = str(payload.get("decision_reason") or "").strip()[:64]
    audit.last_token_balance = int(payload.get("token_balance") or 0)
    audit.last_native_balance = int(payload.get("native_balance") or 0)
    audit.last_token_value_usd = _decimal_or_none(
        payload.get("token_value_usd"), field_name="token_value_usd"
    )
    audit.last_native_value_usd = _decimal_or_none(
        payload.get("native_value_usd"), field_name="native_value_usd"
    )
    audit.last_estimated_token_recovery_cost_usd = _decimal_or_none(
        payload.get("token_recovery_cost_usd"), field_name="token_recovery_cost_usd"
    )
    audit.last_estimated_native_recovery_cost_usd = _decimal_or_none(
        payload.get("native_recovery_cost_usd"), field_name="native_recovery_cost_usd"
    )
    audit.funding_txid = str(payload.get("funding_txid") or "").strip()[:128]
    audit.token_sweep_txid = str(payload.get("token_sweep_txid") or "").strip()[:128]
    audit.native_sweep_txid = str(payload.get("native_sweep_txid") or "").strip()[:128]
    audit.last_error = str(payload.get("error_message") or "").strip()
    audit.metadata = metadata
    audit.last_checked_at = timezone.now()
    audit.finalized_at = timezone.now() if status in TERMINAL_AUDIT_STATUSES else None
    audit.save()
    return audit
