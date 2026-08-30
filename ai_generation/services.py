
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import unicodedata
import uuid
from urllib.parse import urlparse

import requests
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from ledger.models import (
    LEDGER_ACTION_PURCHASE,
    LEDGER_METADATA_VERSION,
    LedgerEntry,
    LedgerTransaction,
    TokenWallet,
)
from ledger.services import (
    _create_outbox_event,
    _require_wallet_not_blocked,
    enforce_wallet_velocity_limits,
    get_system_wallet,
    get_wallet_available_balance,
    consume_promotional_tokens_for_internal_spend,
    record_wallet_velocity,
)

from .models import AIGenerationRequest, AIGenerationRuntimeState


PLATFORM_TOKEN_DECIMALS = 6
DEFAULT_PRICE_TOKENS = 10 * (10 ** PLATFORM_TOKEN_DECIMALS)

MINOR_WORD_RE = re.compile(
    r"\b(?:underage|pre[- ]?teen|child|children|kid|kids|toddler|infant|"
    r"schoolgirl|schoolboy|teen|teenager|loli|shota|csam)\b",
    re.IGNORECASE,
)
AGE_RE = re.compile(
    r"\b(?:aged?\s*)?(\d{1,2})\s*(?:years?\s*old|yo|y/o)\b",
    re.IGNORECASE,
)

HARD_BLOCK_PATTERNS = (
    re.compile(r"\bchild\s*(?:porn|pornography|sexual|sex|nude|naked)\b", re.IGNORECASE),
    re.compile(r"\b(?:rape|raping|sexual\s+assault|forced\s+sex|non[- ]?consensual\s+sex)\b", re.IGNORECASE),
    re.compile(r"\b(?:bestiality|zoophilia|sex\s+with\s+(?:an?\s+)?animal)\b", re.IGNORECASE),
    re.compile(r"\b(?:incest|necrophilia)\b", re.IGNORECASE),
)


def setting_enabled(name: str, default: bool = False) -> bool:
    value = getattr(settings, name, default)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def ai_generation_provider_configured() -> bool:
    url = str(
        getattr(settings, "AI_GENERATION_N8N_WAKE_WEBHOOK_URL", "") or ""
    ).strip()
    secret = str(
        getattr(settings, "AI_GENERATION_N8N_WAKE_SECRET", "") or ""
    ).strip()

    if not url or not secret:
        return False

    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def ai_generation_available() -> bool:
    return (
        setting_enabled("AI_GENERATION_ENABLED", True)
        and ai_generation_provider_configured()
    )


def generation_price_tokens() -> int:
    value = int(getattr(settings, "AI_GENERATION_PRICE_TOKENS", DEFAULT_PRICE_TOKENS))
    if value <= 0:
        raise ValidationError("AI_GENERATION_PRICE_TOKENS must be positive")
    return value


def format_token_amount(value: int) -> str:
    scaled = int(value) / (10 ** PLATFORM_TOKEN_DECIMALS)
    text = f"{scaled:.2f}".rstrip("0").rstrip(".")
    return text or "0"


def get_user_wallet(user) -> TokenWallet:
    wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_USER,
        user=user,
        defaults={"allow_negative": False},
    )
    return wallet


def normalize_prompt(raw_prompt) -> str:
    prompt = unicodedata.normalize("NFKC", str(raw_prompt or ""))
    prompt = re.sub(r"\s+", " ", prompt).strip()
    if not prompt:
        raise ValidationError("Prompt is required")

    max_chars = int(getattr(settings, "AI_GENERATION_MAX_PROMPT_CHARS", 1200))
    if len(prompt) > max_chars:
        raise ValidationError(f"Prompt must be {max_chars} characters or fewer")

    return prompt


def _configured_forbidden_terms() -> tuple[str, ...]:
    value = getattr(settings, "AI_GENERATION_FORBIDDEN_TERMS", ())
    if isinstance(value, str):
        values = value.split("|")
    else:
        values = value
    return tuple(
        unicodedata.normalize("NFKC", str(item)).strip().casefold()
        for item in values
        if str(item).strip()
    )


def moderate_prompt(raw_prompt) -> tuple[str, dict]:
    prompt = normalize_prompt(raw_prompt)
    normalized = prompt.casefold()

    if MINOR_WORD_RE.search(normalized):
        raise ValidationError("Prompt is not allowed by the image generation policy")

    for match in AGE_RE.finditer(normalized):
        age = int(match.group(1))
        if age < 18:
            raise ValidationError("Prompt is not allowed by the image generation policy")

    for pattern in HARD_BLOCK_PATTERNS:
        if pattern.search(normalized):
            raise ValidationError("Prompt is not allowed by the image generation policy")

    for term in _configured_forbidden_terms():
        if term and term in normalized:
            raise ValidationError("Prompt is not allowed by the image generation policy")

    return prompt, {
        "policy_version": 1,
        "checked_at": timezone.now().isoformat(),
        "normalized_length": len(prompt),
    }


def validate_provider_config(*, resolution) -> dict:
    allowed_resolutions = {"512x512", "768x512", "512x768"}

    selected_resolution = str(
        resolution or getattr(settings, "AI_GENERATION_PROVIDER_RESOLUTION", "512x768")
    ).strip()
    if selected_resolution not in allowed_resolutions:
        raise ValidationError("Resolution must be 512x512, 768x512 or 512x768")

    return {
        "resolution": selected_resolution,
        "guidance_scale": 30,
    }



def _request_hash(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _lock_user_wallet(user) -> TokenWallet:
    wallet = get_user_wallet(user)
    return TokenWallet.objects.select_for_update().get(pk=wallet.pk)


def _lock_platform_wallet() -> TokenWallet:
    wallet = get_system_wallet(
        TokenWallet.SYSTEM_PLATFORM_FEES,
        allow_negative=False,
    )
    return TokenWallet.objects.select_for_update().get(pk=wallet.pk)


def _clear_runtime_state_for_generation(generation: AIGenerationRequest) -> None:
    state = (
        AIGenerationRuntimeState.objects.select_for_update()
        .filter(key=AIGenerationRuntimeState.GLOBAL_KEY)
        .first()
    )
    if state and state.current_generation_id == generation.id:
        state.current_generation = None
        state.save(update_fields=["current_generation", "updated_at"])


@transaction.atomic
def create_generation_request(*, actor, prompt, resolution=None) -> AIGenerationRequest:
    # Provider readiness is checked before creating a generation, touching
    # wallets, or recording a ledger transaction. A missing n8n URL/secret must
    # never charge a user for a job that cannot be submitted.
    if not ai_generation_available():
        raise ValidationError("AI image generation is temporarily unavailable")

    if not getattr(actor, "is_authenticated", False) or not getattr(actor, "is_active", False):
        raise ValidationError("Authentication required")

    prompt, moderation = moderate_prompt(prompt)
    requested_provider_config = validate_provider_config(
        resolution=resolution,
    )
    moderation = dict(moderation)
    moderation["requested_provider_config"] = requested_provider_config

    user_model = actor.__class__
    user = user_model.objects.select_for_update().get(pk=actor.pk)

    max_pending = int(getattr(settings, "AI_GENERATION_MAX_PENDING_PER_USER", 3))
    if max_pending > 0:
        pending_count = AIGenerationRequest.objects.filter(
            user=user,
            status__in=[
                AIGenerationRequest.STATUS_QUEUED,
                AIGenerationRequest.STATUS_RUNNING,
            ],
        ).count()
        if pending_count >= max_pending:
            raise ValidationError(
                "You already have too many image generations waiting or running"
            )

    price_tokens = generation_price_tokens()
    generation = AIGenerationRequest.objects.create(
        user=user,
        prompt=prompt,
        moderation=moderation,
        status=AIGenerationRequest.STATUS_QUEUED,
        price_tokens=price_tokens,
        provider=str(getattr(settings, "AI_GENERATION_PROVIDER", "perchance") or "perchance"),
    )

    buyer_wallet = _lock_user_wallet(user)
    platform_wallet = _lock_platform_wallet()

    _require_wallet_not_blocked(buyer_wallet)

    if get_wallet_available_balance(buyer_wallet) < price_tokens:
        raise ValidationError("Insufficient token balance")

    enforce_wallet_velocity_limits(
        wallet=buyer_wallet,
        action=LEDGER_ACTION_PURCHASE,
        amount=price_tokens,
    )

    external_id = f"purchase:ai_generation:{generation.public_id}"
    request_hash = _request_hash(
        {
            "external_id": external_id,
            "generation_public_id": str(generation.public_id),
            "user_id": user.pk,
            "price_tokens": price_tokens,
        }
    )

    promotional_spent = consume_promotional_tokens_for_internal_spend(
        buyer_wallet, price_tokens
    )
    withdrawable_spent = price_tokens - promotional_spent
    buyer_wallet.balance = int(buyer_wallet.balance) - price_tokens
    platform_wallet.balance = int(platform_wallet.balance) + price_tokens
    buyer_wallet.save(
        update_fields=["balance", "promotional_balance", "updated_at"]
    )
    platform_wallet.save(update_fields=["balance", "updated_at"])

    txn = LedgerTransaction.objects.create(
        kind="ai_generation_purchase",
        external_id=external_id,
        request_hash=request_hash,
        created_by=user,
        memo=f"AI image generation {generation.public_id}",
        metadata={
            "product": "ai_generation",
            "generation_public_id": str(generation.public_id),
            "user_id": user.pk,
            "price_tokens": price_tokens,
            "promotional_spent_units": promotional_spent,
            "withdrawable_spent_units": withdrawable_spent,
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

    LedgerEntry.objects.create(
        txn=txn,
        wallet=buyer_wallet,
        delta=-price_tokens,
        promotional_delta=-promotional_spent,
        balance_after=buyer_wallet.balance,
    )
    LedgerEntry.objects.create(
        txn=txn,
        wallet=platform_wallet,
        delta=price_tokens,
        balance_after=platform_wallet.balance,
    )

    record_wallet_velocity(
        wallet=buyer_wallet,
        action=LEDGER_ACTION_PURCHASE,
        amount=price_tokens,
    )

    generation.charge_txn = txn
    generation.charged_at = timezone.now()
    generation.save(update_fields=["charge_txn", "charged_at", "updated_at"])

    _create_outbox_event(
        txn=txn,
        topic="ledger.purchase",
        payload={
            "product": "ai_generation",
            "generation_public_id": str(generation.public_id),
            "user_id": user.pk,
            "price_tokens": price_tokens,
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

    wake_url = str(
        getattr(settings, "AI_GENERATION_N8N_WAKE_WEBHOOK_URL", "") or ""
    ).strip()
    if wake_url:
        from .tasks import wake_ai_generation_worker

        transaction.on_commit(
            lambda generation_id=str(generation.public_id): wake_ai_generation_worker.apply_async(
                args=[generation_id],
                queue="long_tasks",
            )
        )
    return generation



def _mark_generation_failed_locked(
    generation: AIGenerationRequest,
    *,
    error_code: str,
    error_message: str,
    completed_at=None,
) -> AIGenerationRequest:
    generation.status = AIGenerationRequest.STATUS_FAILED
    generation.error_code = str(error_code or "generation_failed")[:64]
    generation.error_message = str(error_message or "")[:2000]
    generation.completed_at = completed_at or timezone.now()
    generation.claimed_by_service = ""
    generation.claim_token = ""
    generation.claim_expires_at = None
    generation.save(
        update_fields=[
            "status",
            "error_code",
            "error_message",
            "completed_at",
            "claimed_by_service",
            "claim_token",
            "claim_expires_at",
            "updated_at",
        ]
    )
    return generation


@transaction.atomic
def claim_next_generation(*, service_name: str) -> AIGenerationRequest | None:
    state, _created = AIGenerationRuntimeState.objects.get_or_create(
        key=AIGenerationRuntimeState.GLOBAL_KEY
    )
    state = AIGenerationRuntimeState.objects.select_for_update().get(pk=state.pk)

    now = timezone.now()
    if state.current_generation_id:
        current = (
            AIGenerationRequest.objects.select_for_update()
            .filter(pk=state.current_generation_id)
            .first()
        )
        if current and current.status == AIGenerationRequest.STATUS_RUNNING:
            if current.claim_expires_at is None or current.claim_expires_at <= now:
                _mark_generation_failed_locked(
                    current,
                    error_code="worker_timeout",
                    error_message="Image generation worker stopped responding.",
                    completed_at=now,
                )
                state.current_generation = None
                state.save(update_fields=["current_generation", "updated_at"])
            else:
                return None
        else:
            state.current_generation = None
            state.save(update_fields=["current_generation", "updated_at"])

    generation = (
        AIGenerationRequest.objects.select_for_update(skip_locked=True)
        .filter(status=AIGenerationRequest.STATUS_QUEUED)
        .order_by("created_at", "id")
        .first()
    )
    if generation is None:
        return None

    now = timezone.now()
    lease_seconds = int(getattr(settings, "AI_GENERATION_CLAIM_LEASE_SECONDS", 300))

    generation.status = AIGenerationRequest.STATUS_RUNNING
    generation.claimed_by_service = str(service_name or "")[:64]
    generation.claim_token = uuid.uuid4().hex
    generation.claim_expires_at = now + timezone.timedelta(seconds=lease_seconds)
    generation.last_heartbeat_at = now
    generation.error_code = ""
    generation.error_message = ""
    generation.save(
        update_fields=[
            "status",
            "claimed_by_service",
            "claim_token",
            "claim_expires_at",
            "last_heartbeat_at",
            "error_code",
            "error_message",
            "updated_at",
        ]
    )

    state.current_generation = generation
    state.save(update_fields=["current_generation", "updated_at"])
    return generation


def _get_claimed_generation_for_update(
    *,
    public_id,
    service_name: str,
    claim_token: str,
) -> AIGenerationRequest:
    generation = (
        AIGenerationRequest.objects.select_for_update()
        .select_related("user")
        .get(public_id=public_id)
    )

    if generation.status != AIGenerationRequest.STATUS_RUNNING:
        raise ValidationError("Generation is not running")
    if generation.claimed_by_service != service_name:
        raise ValidationError("Generation is not claimed by this service")
    if not claim_token or generation.claim_token != claim_token:
        raise ValidationError("Generation claim token does not match")
    if generation.claim_expires_at is None or generation.claim_expires_at <= timezone.now():
        raise ValidationError("Generation claim has expired")

    return generation


@transaction.atomic
def fail_generation(
    *,
    public_id,
    service_name: str,
    claim_token: str,
    error_code: str,
    error_message: str,
) -> AIGenerationRequest:
    generation = _get_claimed_generation_for_update(
        public_id=public_id,
        service_name=service_name,
        claim_token=claim_token,
    )
    _mark_generation_failed_locked(
        generation,
        error_code=error_code,
        error_message=error_message,
    )
    _clear_runtime_state_for_generation(generation)
    return generation


@transaction.atomic
def heartbeat_generation(
    *,
    public_id,
    service_name: str,
    claim_token: str,
) -> AIGenerationRequest:
    generation = _get_claimed_generation_for_update(
        public_id=public_id,
        service_name=service_name,
        claim_token=claim_token,
    )
    now = timezone.now()
    lease_seconds = int(getattr(settings, "AI_GENERATION_CLAIM_LEASE_SECONDS", 300))
    generation.last_heartbeat_at = now
    generation.claim_expires_at = now + timezone.timedelta(seconds=lease_seconds)
    generation.save(
        update_fields=[
            "last_heartbeat_at",
            "claim_expires_at",
            "updated_at",
        ]
    )
    return generation


def _allowed_result_hosts() -> set[str]:
    configured = getattr(
        settings,
        "AI_GENERATION_ALLOWED_RESULT_HOSTS",
        ("image-generation.perchance.org",),
    )
    if isinstance(configured, str):
        values = configured.split(",")
    else:
        values = configured
    return {str(item).strip().lower() for item in values if str(item).strip()}


def _validate_result_url(url: str) -> None:
    parsed = urlparse(str(url or ""))
    if parsed.scheme != "https":
        raise ValidationError("Provider result URL must use HTTPS")
    if not parsed.hostname or parsed.hostname.lower() not in _allowed_result_hosts():
        raise ValidationError("Provider result host is not allowed")
    if parsed.username or parsed.password:
        raise ValidationError("Provider result URL credentials are not allowed")


def download_provider_image(url: str) -> tuple[bytes, str, str]:
    _validate_result_url(url)
    timeout_seconds = int(getattr(settings, "AI_GENERATION_RESULT_DOWNLOAD_TIMEOUT_SECONDS", 30))
    max_bytes = int(getattr(settings, "AI_GENERATION_RESULT_MAX_BYTES", 20 * 1024 * 1024))

    try:
        with requests.get(
            url,
            stream=True,
            timeout=timeout_seconds,
            allow_redirects=False,
        ) as response:
            if 300 <= response.status_code < 400:
                raise ValidationError("Provider result redirects are not allowed")
            response.raise_for_status()
            _validate_result_url(response.url)

            content_type = (
                response.headers.get("Content-Type", "")
                .split(";", 1)[0]
                .strip()
                .lower()
            )
            extensions = {
                "image/jpeg": "jpg",
                "image/png": "png",
                "image/webp": "webp",
            }
            extension = extensions.get(content_type)
            if extension is None:
                raise ValidationError("Provider returned an unsupported image type")

            chunks = []
            total = 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_bytes:
                    raise ValidationError("Provider image exceeds configured size limit")
                chunks.append(chunk)
    except requests.RequestException as exc:
        raise ValidationError("Could not download provider image") from exc

    if total <= 0:
        raise ValidationError("Provider returned an empty image")

    return b"".join(chunks), content_type, extension



def decode_provider_image_base64(
    image_base64: str,
    content_type: str,
) -> tuple[bytes, str]:
    normalized_type = str(content_type or "").split(";", 1)[0].strip().lower()
    extensions = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
    }
    extension = extensions.get(normalized_type)
    if extension is None:
        raise ValidationError("Provider returned an unsupported image type")

    raw_value = str(image_base64 or "").strip()
    if raw_value.startswith("data:"):
        try:
            header, raw_value = raw_value.split(",", 1)
        except ValueError as exc:
            raise ValidationError("Generated image data URL is invalid") from exc
        header_type = header[5:].split(";", 1)[0].strip().lower()
        if header_type and header_type != normalized_type:
            raise ValidationError("Generated image content type does not match")

    try:
        image_bytes = base64.b64decode(raw_value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValidationError("Generated image base64 is invalid") from exc

    max_bytes = int(
        getattr(settings, "AI_GENERATION_RESULT_MAX_BYTES", 20 * 1024 * 1024)
    )
    if not image_bytes:
        raise ValidationError("Generated image is empty")
    if len(image_bytes) > max_bytes:
        raise ValidationError("Generated image exceeds configured size limit")

    return image_bytes, extension


def complete_generation(
    *,
    public_id,
    service_name: str,
    claim_token: str,
    image_bytes: bytes,
    content_type: str,
    extension: str,
    provider_request_id: str = "",
    provider_metadata: dict | None = None,
) -> AIGenerationRequest:
    # Inline/base64 completion used to persist files under MEDIA_ROOT. The
    # production one-way provider contract is URL-only and intentionally
    # diskless, so inline image completion is no longer accepted.
    raise ValidationError(
        "Inline image results are no longer supported; provide a result URL"
    )



@transaction.atomic
def complete_generation_from_url(
    *,
    public_id,
    service_name: str,
    claim_token: str,
    result_url: str,
    provider_request_id: str = "",
    provider_metadata: dict | None = None,
) -> AIGenerationRequest:
    # The active one-way provider flow is intentionally diskless. Persist only
    # the validated provider URL/metadata. The image itself is fetched on demand
    # by generation_image() and streamed to the browser without touching
    # MEDIA_ROOT.
    _validate_result_url(result_url)

    generation = _get_claimed_generation_for_update(
        public_id=public_id,
        service_name=service_name,
        claim_token=claim_token,
    )

    metadata = (
        dict(provider_metadata)
        if isinstance(provider_metadata, dict)
        else {}
    )
    metadata["image_download_url"] = str(result_url)

    generation.result_file = ""
    generation.result_content_type = ""
    generation.result_metadata = metadata
    generation.provider_request_id = str(provider_request_id or "")[:255]
    generation.status = AIGenerationRequest.STATUS_SUCCESS
    generation.completed_at = timezone.now()
    generation.claimed_by_service = ""
    generation.claim_token = ""
    generation.claim_expires_at = None
    generation.error_code = ""
    generation.error_message = ""
    generation.save(
        update_fields=[
            "result_file",
            "result_content_type",
            "result_metadata",
            "provider_request_id",
            "status",
            "completed_at",
            "claimed_by_service",
            "claim_token",
            "claim_expires_at",
            "error_code",
            "error_message",
            "updated_at",
        ]
    )
    _clear_runtime_state_for_generation(generation)
    return generation


def generation_provider_payload(generation: AIGenerationRequest) -> dict:
    requested = generation.moderation.get("requested_provider_config", {})
    if not isinstance(requested, dict):
        requested = {}

    provider_config = validate_provider_config(
        resolution=requested.get("resolution"),
    )

    return {
        "public_id": str(generation.public_id),
        "claim_token": generation.claim_token,
        "prompt": generation.prompt,
        "provider": generation.provider,
        "provider_config": provider_config,
    }


def serialize_generation(generation: AIGenerationRequest, *, request=None) -> dict:
    image_url = ""
    provider_result_url = ""
    if isinstance(generation.result_metadata, dict):
        provider_result_url = str(
            generation.result_metadata.get("image_download_url", "") or ""
        ).strip()

    if (
        generation.status == AIGenerationRequest.STATUS_SUCCESS
        and (provider_result_url or generation.result_file)
    ):
        path = reverse(
            "ai_generation_image",
            kwargs={"public_id": generation.public_id},
        )
        image_url = request.build_absolute_uri(path) if request is not None else path

    requested = generation.moderation.get("requested_provider_config", {})
    if not isinstance(requested, dict):
        requested = {}

    return {
        "id": str(generation.public_id),
        "status": generation.status,
        "prompt": generation.prompt,
        "price_tokens": int(generation.price_tokens),
        "price_display": format_token_amount(generation.price_tokens),
        "image_url": image_url,
        "error_code": generation.error_code,
        "error_message": generation.error_message,
        "resolution": str(
            requested.get(
                "resolution",
                getattr(settings, "AI_GENERATION_PROVIDER_RESOLUTION", "512x768"),
            )
        ),
        "guidance_scale": 30,
        "created_at": generation.created_at.isoformat(),
        "completed_at": (
            generation.completed_at.isoformat()
            if generation.completed_at
            else None
        ),
    }


@transaction.atomic
def _fail_stale_generation(
    *,
    public_id,
    expected_status: str,
    error_code: str,
    error_message: str,
) -> bool:
    generation = (
        AIGenerationRequest.objects.select_for_update()
        .filter(public_id=public_id, status=expected_status)
        .first()
    )
    if generation is None:
        return False

    now = timezone.now()
    if expected_status == AIGenerationRequest.STATUS_RUNNING:
        # A heartbeat may have renewed the lease after the stale-id scan.
        if generation.claim_expires_at is not None and generation.claim_expires_at > now:
            return False

    _mark_generation_failed_locked(
        generation,
        error_code=error_code,
        error_message=error_message,
        completed_at=now,
    )
    _clear_runtime_state_for_generation(generation)
    return True


def fail_stale_generations() -> dict:
    now = timezone.now()
    queue_timeout = int(
        getattr(settings, "AI_GENERATION_QUEUE_TIMEOUT_SECONDS", 30 * 60)
    )

    stale_ids = list(
        AIGenerationRequest.objects.filter(
            status=AIGenerationRequest.STATUS_RUNNING,
            claim_expires_at__isnull=False,
            claim_expires_at__lt=now,
        ).values_list("public_id", flat=True)
    )

    stale_queue_ids = list(
        AIGenerationRequest.objects.filter(
            status=AIGenerationRequest.STATUS_QUEUED,
            created_at__lt=now - timezone.timedelta(seconds=queue_timeout),
        ).values_list("public_id", flat=True)
    )

    failed_running = 0
    failed_queued = 0

    for public_id in stale_ids:
        if _fail_stale_generation(
            public_id=public_id,
            expected_status=AIGenerationRequest.STATUS_RUNNING,
            error_code="worker_timeout",
            error_message="Image generation worker stopped responding.",
        ):
            failed_running += 1

    for public_id in stale_queue_ids:
        if _fail_stale_generation(
            public_id=public_id,
            expected_status=AIGenerationRequest.STATUS_QUEUED,
            error_code="queue_timeout",
            error_message="Image generation could not start in time.",
        ):
            failed_queued += 1

    return {
        "failed_running": failed_running,
        "failed_queued": failed_queued,
    }
