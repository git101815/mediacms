from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import re
import secrets
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from urllib.parse import urlparse

from django.conf import settings
from django.core import signing
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Q, Sum
from django.urls import reverse
from django.utils import timezone

from actions.models import MediaAction
from files.models import Comment, Media
from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet

from . import config
from .models import (
    QuestOwnerIdentity,
    QuestQualifiedVisit,
    QuestShareCampaign,
    RewardChestGrant,
)
from .quests import build_quest_board_context as build_starter_quest_board_context
from .reward_chests import grant_reward_chest, open_reward_chest


VISITOR_COOKIE_NAME = "cfq_vid"
ATTRIBUTION_COOKIE_NAME = "cfq_attr"
ATTRIBUTION_SIGNING_SALT = "ledger.quest-board.share-attribution.v1"
FINGERPRINT_RE = re.compile(r"^[a-f0-9]{16,128}$")
CYCLE_KEY_RE = re.compile(r"^(?P<year>\d{4})-W(?P<week>\d{2})$")


@dataclass(frozen=True)
class QuestCycle:
    key: str
    starts_at: datetime
    ends_at: datetime
    rotation_index: int


@dataclass(frozen=True)
class WeeklyQuestDefinition:
    key: str
    title: str
    description: str
    condition: str
    icon_path: str
    icon_material: str
    action_label: str
    action_url: str
    target: int
    chest_key: str
    chest_label: str
    chest_image_path: str
    platform: str
    personal_target: int
    global_target: int
    landing_path: str
    progress_text: str
    progress_pending_text: str
    progress_complete_text: str


def _weekly_enabled() -> bool:
    return bool(config.QUEST_BOARD_ENABLED and config.QUEST_BOARD_WEEKLY_ENABLED)


def _require_user(user):
    if not getattr(user, "is_authenticated", False):
        raise PermissionDenied("Authentication is required")
    if not getattr(user, "pk", None):
        raise PermissionDenied("A persisted user account is required")
    if not getattr(user, "is_active", False):
        raise PermissionDenied("Inactive accounts cannot use quests")
    return user


def _positive_int(value, *, name: str, allow_zero: bool = False) -> int:
    if isinstance(value, bool):
        raise ImproperlyConfigured(f"{name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured(f"{name} must be an integer") from exc
    minimum = 0 if allow_zero else 1
    if parsed < minimum:
        raise ImproperlyConfigured(f"{name} must be at least {minimum}")
    return parsed


def _quest_board_start_at() -> datetime:
    raw = str(config.QUEST_BOARD_WEEKLY_START_AT or "").strip()
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ImproperlyConfigured(
            "QUEST_BOARD_WEEKLY_START_AT must be ISO-8601"
        ) from exc
    if timezone.is_naive(value):
        value = timezone.make_aware(value, timezone.get_current_timezone())
    return value


def _cycle_for_datetime(at=None) -> QuestCycle:
    current = timezone.localtime(at or timezone.now())
    starts_at = (current - timedelta(days=current.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    ends_at = starts_at + timedelta(days=7)
    iso = starts_at.isocalendar()
    return QuestCycle(
        key=f"{iso.year}-W{iso.week:02d}",
        starts_at=starts_at,
        ends_at=ends_at,
        rotation_index=0,
    )


def _cycle_from_key(cycle_key: str) -> QuestCycle:
    normalized = str(cycle_key or "").strip().upper()
    match = CYCLE_KEY_RE.fullmatch(normalized)
    if not match:
        raise ValidationError("Invalid quest cycle")
    try:
        monday = date.fromisocalendar(
            int(match.group("year")),
            int(match.group("week")),
            1,
        )
    except ValueError as exc:
        raise ValidationError("Invalid quest cycle") from exc
    starts_at = timezone.make_aware(
        datetime.combine(monday, time.min),
        timezone.get_current_timezone(),
    )
    return _cycle_for_datetime(starts_at + timedelta(hours=12))


def _active_keys(*, cycle: QuestCycle, user) -> tuple[str, ...]:
    user = _require_user(user)
    definitions = config.QUEST_BOARD_WEEKLY_QUESTS
    if not isinstance(definitions, dict):
        raise ImproperlyConfigured("QUEST_BOARD_WEEKLY_QUESTS must be a dictionary")

    enabled_keys = []
    for key, raw in definitions.items():
        if not isinstance(raw, dict):
            raise ImproperlyConfigured(f"Weekly quest {key} must be a dictionary")
        enabled = raw.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ImproperlyConfigured(f"Quest {key}.enabled must be True or False")
        if enabled:
            enabled_keys.append(str(key))

    slot_count = int(config.QUEST_BOARD_SLOT_COUNT)
    if slot_count < 1:
        raise ImproperlyConfigured("QUEST_BOARD_SLOT_COUNT must be positive")
    if not enabled_keys:
        return ()

    selection_salt = str(config.QUEST_BOARD_WEEKLY_SELECTION_SALT or "").strip()
    if not selection_salt:
        raise ImproperlyConfigured("QUEST_BOARD_WEEKLY_SELECTION_SALT cannot be empty")

    seed_prefix = f"{selection_salt}:{cycle.key}:user:{int(user.pk)}"

    def rank(quest_key: str):
        return hmac.new(
            settings.SECRET_KEY.encode("utf-8"),
            f"{seed_prefix}:quest:{quest_key}".encode("utf-8"),
            hashlib.sha256,
        ).digest()

    ordered = sorted(enabled_keys, key=lambda key: (rank(key), key))
    return tuple(ordered[:slot_count])


def _definition_from_config(key: str) -> WeeklyQuestDefinition:
    definitions = config.QUEST_BOARD_WEEKLY_QUESTS
    if not isinstance(definitions, dict):
        raise ImproperlyConfigured("QUEST_BOARD_WEEKLY_QUESTS must be a dictionary")
    raw = definitions.get(key)
    if not isinstance(raw, dict):
        raise ImproperlyConfigured(f"Unknown weekly quest: {key}")

    enabled = raw.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ImproperlyConfigured(f"Quest {key}.enabled must be True or False")
    if not enabled:
        raise ImproperlyConfigured(f"Quest {key} is disabled")

    condition = str(raw.get("condition") or "").strip()
    if condition not in {
        "site_visitors",
        "video_share",
        "community_likes",
        "community_views",
        "community_comments",
        "community_spend",
    }:
        raise ImproperlyConfigured(
            f"Quest {key} uses unsupported condition: {condition}"
        )

    assets = config.get_wallet_asset_paths()
    icon_asset = str(raw.get("icon_asset") or "").strip()
    icon_material = str(raw.get("icon_material") or "").strip()

    if icon_asset and icon_material:
        raise ImproperlyConfigured(
            f"Quest {key} must define either icon_asset or icon_material, not both"
        )

    if not icon_asset and not icon_material:
        raise ImproperlyConfigured(
            f"Quest {key} must define icon_asset or icon_material"
        )

    if icon_material:
        if not re.fullmatch(r"[a-z0-9_]{1,64}", icon_material):
            raise ImproperlyConfigured(
                f"Quest {key}.icon_material is invalid"
            )
        icon_path = ""
    else:
        if icon_asset not in assets:
            raise ImproperlyConfigured(
                f"Quest {key} references unknown wallet asset: {icon_asset}"
            )
        icon_path = assets[icon_asset]

    reward = raw.get("reward") or {}
    if not isinstance(reward, dict) or reward.get("kind") != "chest":
        raise ImproperlyConfigured(f"Quest {key} must use a chest reward")
    chest = config.get_reward_chest_definition(str(reward.get("chest") or ""))

    target = _positive_int(raw.get("target", 1), name=f"{key}.target")
    personal_target = _positive_int(
        raw.get("personal_target", target),
        name=f"{key}.personal_target",
    )
    global_target = _positive_int(
        raw.get("global_target", 0),
        name=f"{key}.global_target",
        allow_zero=not condition.startswith("community_"),
    )

    platform = str(raw.get("platform") or "").strip().lower()
    if condition == "video_share" and platform not in config.QUEST_BOARD_SOCIAL_HOSTS:
        raise ImproperlyConfigured(
            f"Quest {key} references unknown social platform: {platform}"
        )

    progress_text = str(raw.get("progress_text") or "").strip()
    progress_pending_text = str(raw.get("progress_pending_text") or "").strip()
    progress_complete_text = str(raw.get("progress_complete_text") or "").strip()

    if condition == "video_share":
        if not progress_pending_text:
            raise ImproperlyConfigured(
                f"Quest {key}.progress_pending_text cannot be empty"
            )
        if not progress_complete_text:
            raise ImproperlyConfigured(
                f"Quest {key}.progress_complete_text cannot be empty"
            )
    elif not progress_text:
        raise ImproperlyConfigured(f"Quest {key}.progress_text cannot be empty")

    return WeeklyQuestDefinition(
        key=key,
        title=str(raw.get("title") or key).strip(),
        description=str(raw.get("description") or "").strip(),
        condition=condition,
        icon_path=icon_path,
        icon_material=icon_material,
        action_label=str(raw.get("action_label") or "Go").strip(),
        action_url=str(raw.get("action_url") or "").strip(),
        target=target,
        chest_key=chest.key,
        chest_label=chest.label,
        chest_image_path=chest.closed_image,
        platform=platform,
        personal_target=personal_target,
        global_target=global_target,
        landing_path=str(raw.get("landing_path") or "/").strip() or "/",
        progress_text=progress_text,
        progress_pending_text=progress_pending_text,
        progress_complete_text=progress_complete_text,
    )


def get_weekly_definitions(
    *,
    user,
    cycle: QuestCycle | None = None,
) -> tuple[WeeklyQuestDefinition, ...]:
    cycle = cycle or _cycle_for_datetime()
    return tuple(
        _definition_from_config(key)
        for key in _active_keys(cycle=cycle, user=user)
    )


def _fingerprint_hash(raw: str) -> str:
    normalized = str(raw or "").strip().lower()
    if not FINGERPRINT_RE.fullmatch(normalized):
        raise ValidationError("A valid browser fingerprint is required")
    return _private_hash("fingerprint", normalized)


def _private_hash(namespace: str, value: str) -> str:
    return hmac.new(
        settings.SECRET_KEY.encode("utf-8"),
        f"{namespace}:{value}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _client_ip(request) -> str:
    value = (
        request.META.get("HTTP_CF_CONNECTING_IP")
        or (request.META.get("HTTP_X_FORWARDED_FOR") or "").split(",", 1)[0]
        or request.META.get("REMOTE_ADDR")
        or ""
    ).strip()
    try:
        return str(ipaddress.ip_address(value))
    except ValueError as exc:
        raise ValidationError("A valid client IP is required") from exc


def _network_hash(request) -> str:
    address = ipaddress.ip_address(_client_ip(request))
    if address.version == 6:
        normalized = str(ipaddress.ip_network(f"{address}/64", strict=False))
    else:
        normalized = str(address)
    return _private_hash("network", normalized)


def _visitor_token(request) -> tuple[str, bool]:
    raw = str(request.COOKIES.get(VISITOR_COOKIE_NAME) or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{32,128}", raw):
        return raw, False
    return secrets.token_urlsafe(32), True


def _visitor_hash(token: str) -> str:
    return _private_hash("visitor", token)


def set_visitor_cookie(response, *, request, token: str):
    response.set_cookie(
        VISITOR_COOKIE_NAME,
        token,
        max_age=int(config.QUEST_BOARD_VISITOR_COOKIE_SECONDS),
        secure=bool(not settings.DEBUG or request.is_secure()),
        httponly=True,
        samesite="Lax",
    )
    return response


def _register_owner_identity(*, request, user, fingerprint: str, cycle: QuestCycle):
    visitor_token, _created = _visitor_token(request)
    QuestOwnerIdentity.objects.get_or_create(
        user=user,
        cycle_key=cycle.key,
        network_hash=_network_hash(request),
        fingerprint_hash=_fingerprint_hash(fingerprint),
        visitor_hash=_visitor_hash(visitor_token),
    )
    return visitor_token


def _campaign_key(*parts) -> str:
    raw = ":".join(str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _campaign_url(request, campaign: QuestShareCampaign) -> str:
    return request.build_absolute_uri(
        reverse("weekly_quest_redirect", kwargs={"public_id": campaign.public_id})
    )


def create_site_share_link(*, request, user, fingerprint: str) -> dict:
    user = _require_user(user)
    cycle = _cycle_for_datetime()
    if not _weekly_enabled():
        raise ValidationError("Weekly quests are disabled")
    if timezone.now() < _quest_board_start_at():
        raise ValidationError("Weekly quests have not started")
    definition = next(
        (row for row in get_weekly_definitions(user=user, cycle=cycle) if row.condition == "site_visitors"),
        None,
    )
    if definition is None:
        raise ValidationError("The site sharing quest is not active this week")
    visitor_token = _register_owner_identity(
        request=request,
        user=user,
        fingerprint=fingerprint,
        cycle=cycle,
    )
    campaign, _created = QuestShareCampaign.objects.get_or_create(
        campaign_key=_campaign_key(user.pk, cycle.key, definition.key, "site"),
        defaults={
            "owner": user,
            "cycle_key": cycle.key,
            "quest_key": definition.key,
            "campaign_type": QuestShareCampaign.TYPE_SITE,
            "target_path": definition.landing_path,
        },
    )
    return {
        "url": _campaign_url(request, campaign),
        "visitor_token": visitor_token,
    }


def create_video_share_link(
    *, request, user, fingerprint: str, media_token: str, platform: str
) -> dict:
    user = _require_user(user)
    cycle = _cycle_for_datetime()
    platform = str(platform or "").strip().lower()
    media = Media.objects.filter(
        friendly_token=str(media_token or "").strip(),
        media_type="video",
        state="public",
    ).first()
    if media is None:
        raise ValidationError("A public video is required")

    fallback_url = request.build_absolute_uri(media.get_absolute_url())
    if not _weekly_enabled():
        return {"url": fallback_url, "tracked": False, "visitor_token": None}
    definition = next(
        (
            row
            for row in get_weekly_definitions(user=user, cycle=cycle)
            if row.condition == "video_share" and row.platform == platform
        ),
        None,
    )
    if definition is None or timezone.now() < _quest_board_start_at():
        return {"url": fallback_url, "tracked": False, "visitor_token": None}

    visitor_token = _register_owner_identity(
        request=request,
        user=user,
        fingerprint=fingerprint,
        cycle=cycle,
    )
    campaign, _created = QuestShareCampaign.objects.get_or_create(
        campaign_key=_campaign_key(
            user.pk,
            cycle.key,
            definition.key,
            platform,
            media.pk,
        ),
        defaults={
            "owner": user,
            "cycle_key": cycle.key,
            "quest_key": definition.key,
            "campaign_type": QuestShareCampaign.TYPE_VIDEO,
            "expected_platform": platform,
            "media": media,
            "target_path": media.get_absolute_url(),
        },
    )
    return {
        "url": _campaign_url(request, campaign),
        "tracked": True,
        "visitor_token": visitor_token,
    }


def _referer_host(request) -> str:
    value = str(request.META.get("HTTP_REFERER") or "").strip()
    return (urlparse(value).hostname or "").lower()


def _host_matches_platform(host: str, platform: str) -> bool:
    for expected in config.QUEST_BOARD_SOCIAL_HOSTS.get(platform, ()):  # exact or subdomain
        expected = str(expected or "").strip().lower()
        if host == expected or host.endswith(f".{expected}"):
            return True
    return False


def _is_platform_unfurl(request, platform: str) -> bool:
    user_agent = str(request.META.get("HTTP_USER_AGENT") or "").lower()
    return any(
        str(fragment or "").strip().lower() in user_agent
        for fragment in config.QUEST_BOARD_UNFURL_USER_AGENTS.get(platform, ())
        if str(fragment or "").strip()
    )


def _video_platform_verified(request, campaign: QuestShareCampaign) -> bool:
    host = _referer_host(request)
    if host and _host_matches_platform(host, campaign.expected_platform):
        return True
    if campaign.preview_seen_at is None:
        return False
    window = timedelta(seconds=int(config.QUEST_BOARD_UNFURL_WINDOW_SECONDS))
    return campaign.preview_seen_at >= timezone.now() - window


def build_share_redirect_response(*, request, public_id):
    from django.shortcuts import get_object_or_404, redirect

    campaign = get_object_or_404(
        QuestShareCampaign.objects.select_related("media"),
        public_id=public_id,
    )
    target = campaign.target_path or "/"
    current_cycle = _cycle_for_datetime()
    if (
        not _weekly_enabled()
        or campaign.cycle_key != current_cycle.key
        or timezone.now() >= current_cycle.ends_at
    ):
        return redirect(target)

    if (
        campaign.campaign_type == QuestShareCampaign.TYPE_VIDEO
        and _is_platform_unfurl(request, campaign.expected_platform)
    ):
        campaign.preview_seen_at = timezone.now()
        campaign.save(update_fields=["preview_seen_at"])
        return redirect(target)

    visitor_token, _created = _visitor_token(request)
    platform_verified = (
        campaign.campaign_type == QuestShareCampaign.TYPE_SITE
        or _video_platform_verified(request, campaign)
    )
    attribution = signing.dumps(
        {
            "campaign": str(campaign.public_id),
            "cycle": campaign.cycle_key,
            "landing": target,
            "issued_at": int(timezone.now().timestamp()),
            "platform_verified": bool(platform_verified),
            "referer_host": _referer_host(request),
        },
        salt=ATTRIBUTION_SIGNING_SALT,
        compress=True,
    )
    response = redirect(target)
    response.set_cookie(
        ATTRIBUTION_COOKIE_NAME,
        attribution,
        max_age=int(config.QUEST_BOARD_ATTRIBUTION_COOKIE_SECONDS),
        secure=bool(not settings.DEBUG or request.is_secure()),
        httponly=True,
        samesite="Lax",
    )
    return set_visitor_cookie(response, request=request, token=visitor_token)


def _load_attribution(request) -> dict | None:
    raw = str(request.COOKIES.get(ATTRIBUTION_COOKIE_NAME) or "").strip()
    if not raw:
        return None
    try:
        payload = signing.loads(
            raw,
            salt=ATTRIBUTION_SIGNING_SALT,
            max_age=int(config.QUEST_BOARD_ATTRIBUTION_COOKIE_SECONDS),
        )
    except signing.BadSignature:
        return None
    return payload if isinstance(payload, dict) else None


def _normalize_page(raw: str) -> str:
    page = str(raw or "").strip()
    if not page.startswith("/") or len(page) > 500:
        raise ValidationError("Invalid page")
    return page.split("#", 1)[0]


def _page_is_eligible(page: str) -> bool:
    return not any(
        page.startswith(str(prefix))
        for prefix in config.QUEST_BOARD_EXCLUDED_PAGE_PREFIXES
    )


def _owner_identity_matches(
    *, campaign: QuestShareCampaign, network_hash: str, fingerprint_hash: str, visitor_hash: str
) -> bool:
    return QuestOwnerIdentity.objects.filter(
        user_id=campaign.owner_id,
        cycle_key=campaign.cycle_key,
    ).filter(
        Q(network_hash=network_hash)
        | Q(fingerprint_hash=fingerprint_hash)
        | Q(visitor_hash=visitor_hash)
    ).exists()


def _definition_for_campaign(campaign: QuestShareCampaign) -> WeeklyQuestDefinition:
    cycle = _cycle_from_key(campaign.cycle_key)
    for definition in get_weekly_definitions(user=campaign.owner, cycle=cycle):
        if definition.key == campaign.quest_key:
            return definition
    raise ValidationError("The campaign quest is not active")


def _format_progress_text(
    *,
    definition: WeeklyQuestDefinition,
    template: str,
    values: dict,
) -> str:
    try:
        return template.format(**values)
    except (KeyError, IndexError, ValueError) as exc:
        raise ImproperlyConfigured(
            f"Quest {definition.key} has an invalid progress text template"
        ) from exc


def _metric_window(cycle: QuestCycle) -> tuple[datetime, datetime]:
    return max(cycle.starts_at, _quest_board_start_at()), cycle.ends_at


def _community_spend_kinds() -> tuple[str, ...]:
    raw = config.QUEST_BOARD_COMMUNITY_SPEND_TRANSACTION_KINDS
    if isinstance(raw, str) or not isinstance(raw, (list, tuple, set, frozenset)):
        raise ImproperlyConfigured(
            "QUEST_BOARD_COMMUNITY_SPEND_TRANSACTION_KINDS must be a collection"
        )
    kinds = tuple(dict.fromkeys(str(value or "").strip().lower() for value in raw))
    if not kinds or any(not value for value in kinds):
        raise ImproperlyConfigured(
            "QUEST_BOARD_COMMUNITY_SPEND_TRANSACTION_KINDS cannot be empty"
        )
    return kinds


def _spent_token_totals(*, user, cycle: QuestCycle) -> tuple[int, int]:
    starts_at, ends_at = _metric_window(cycle)
    filters = {
        "wallet__wallet_type": TokenWallet.TYPE_USER,
        "txn__kind__in": _community_spend_kinds(),
        "txn__status": LedgerTransaction.STATUS_POSTED,
        "delta__lt": 0,
        "created_at__gte": starts_at,
        "created_at__lt": ends_at,
    }
    global_signed = LedgerEntry.objects.filter(**filters).aggregate(total=Sum("delta")).get("total")
    personal_signed = LedgerEntry.objects.filter(wallet__user=user, **filters).aggregate(total=Sum("delta")).get("total")
    scale = 10 ** int(config.PLATFORM_TOKEN_DECIMALS)
    personal_tokens = max(0, -int(personal_signed or 0)) // scale
    global_tokens = max(0, -int(global_signed or 0)) // scale
    return personal_tokens, global_tokens


def _community_metric_values(*, user, cycle: QuestCycle, condition: str) -> tuple[int, int]:
    starts_at, ends_at = _metric_window(cycle)
    if condition in {"community_likes", "community_views"}:
        action = "like" if condition == "community_likes" else "watch"
        queryset = MediaAction.objects.filter(
            action=action,
            action_date__gte=starts_at,
            action_date__lt=ends_at,
        )
        return queryset.filter(user=user).count(), queryset.count()
    if condition == "community_comments":
        queryset = Comment.objects.filter(add_date__gte=starts_at, add_date__lt=ends_at)
        return queryset.filter(user=user).count(), queryset.count()
    if condition == "community_spend":
        return _spent_token_totals(user=user, cycle=cycle)
    raise ImproperlyConfigured(f"Unsupported community quest condition: {condition}")


def _community_progress(*, user, cycle: QuestCycle, definition: WeeklyQuestDefinition) -> dict:
    personal, global_current = _community_metric_values(
        user=user,
        cycle=cycle,
        condition=definition.condition,
    )
    personal_display = min(personal, definition.personal_target)
    global_display = min(global_current, definition.global_target)
    personal_percent = min(100, personal * 100 // definition.personal_target)
    global_percent = min(100, global_current * 100 // definition.global_target)
    complete = personal >= definition.personal_target and global_current >= definition.global_target
    return {
        "current": personal_display,
        "target": definition.personal_target,
        "complete": complete,
        "progress_percent": min(personal_percent, global_percent),
        "progress_text": _format_progress_text(
            definition=definition,
            template=definition.progress_text,
            values={
                "personal_current": personal,
                "personal_target": definition.personal_target,
                "global_current": global_display,
                "global_target": definition.global_target,
            },
        ),
    }


def _quest_progress(*, user, cycle: QuestCycle, definition: WeeklyQuestDefinition) -> dict:
    own_visits = QuestQualifiedVisit.objects.filter(cycle_key=cycle.key, campaign__owner=user)
    if definition.condition == "site_visitors":
        current = own_visits.filter(
            campaign__quest_key=definition.key,
            qualification_type=QuestQualifiedVisit.TYPE_SITE_SECOND_PAGE,
        ).count()
        displayed_current = min(current, definition.target)
        complete = current >= definition.target
        return {
            "current": displayed_current,
            "target": definition.target,
            "complete": complete,
            "progress_percent": min(100, current * 100 // definition.target),
            "progress_text": _format_progress_text(
                definition=definition,
                template=definition.progress_text,
                values={"current": displayed_current, "target": definition.target},
            ),
        }
    if definition.condition == "video_share":
        current = int(
            own_visits.filter(
                campaign__quest_key=definition.key,
                qualification_type=QuestQualifiedVisit.TYPE_VIDEO_PLATFORM,
            ).exists()
        )
        complete = bool(current)
        return {
            "current": current,
            "target": definition.target,
            "complete": complete,
            "progress_percent": 100 if complete else 0,
            "progress_text": definition.progress_complete_text if complete else definition.progress_pending_text,
        }
    if definition.condition.startswith("community_"):
        return _community_progress(user=user, cycle=cycle, definition=definition)
    raise ImproperlyConfigured(f"Unsupported weekly quest condition: {definition.condition}")


def _grant_source_ref(*, user_id: int, cycle_key: str, quest_key: str) -> str:
    return f"weekly-quest:{cycle_key}:user:{int(user_id)}:quest:{quest_key}"


def _ensure_reward_grant(*, user, cycle: QuestCycle, definition: WeeklyQuestDefinition):
    return grant_reward_chest(
        user=user,
        chest_key=definition.chest_key,
        source_type="weekly_quest",
        source_ref=_grant_source_ref(
            user_id=user.pk,
            cycle_key=cycle.key,
            quest_key=definition.key,
        ),
        metadata={
            "cycle_key": cycle.key,
            "quest_key": definition.key,
            "quest_title": definition.title,
        },
    )


def _grant_completed_campaign_quest(campaign: QuestShareCampaign):
    cycle = _cycle_from_key(campaign.cycle_key)
    definition = _definition_for_campaign(campaign)
    progress = _quest_progress(
        user=campaign.owner,
        cycle=cycle,
        definition=definition,
    )
    if progress["complete"]:
        _ensure_reward_grant(
            user=campaign.owner,
            cycle=cycle,
            definition=definition,
        )


@transaction.atomic
def record_navigation(*, request, fingerprint: str, page: str) -> dict:
    cycle = _cycle_for_datetime()
    page = _normalize_page(page)
    visitor_token, _created = _visitor_token(request)
    network_hash = _network_hash(request)
    browser_hash = _fingerprint_hash(fingerprint)
    browser_visitor_hash = _visitor_hash(visitor_token)

    if getattr(request.user, "is_authenticated", False):
        QuestOwnerIdentity.objects.get_or_create(
            user=request.user,
            cycle_key=cycle.key,
            network_hash=network_hash,
            fingerprint_hash=browser_hash,
            visitor_hash=browser_visitor_hash,
        )

    attribution = _load_attribution(request)
    if not attribution or attribution.get("cycle") != cycle.key:
        return {"qualified": False, "visitor_token": visitor_token, "clear_attribution": False}

    campaign = QuestShareCampaign.objects.select_related("owner").filter(
        public_id=attribution.get("campaign"),
        cycle_key=cycle.key,
    ).first()
    if campaign is None:
        return {"qualified": False, "visitor_token": visitor_token, "clear_attribution": True}

    if _owner_identity_matches(
        campaign=campaign,
        network_hash=network_hash,
        fingerprint_hash=browser_hash,
        visitor_hash=browser_visitor_hash,
    ):
        return {"qualified": False, "visitor_token": visitor_token, "clear_attribution": True}

    landing = _normalize_page(attribution.get("landing") or "/")
    qualification_type = ""
    second_page = ""
    if campaign.campaign_type == QuestShareCampaign.TYPE_SITE:
        elapsed = int(timezone.now().timestamp()) - int(attribution.get("issued_at") or 0)
        if (
            page == landing
            or not _page_is_eligible(page)
            or elapsed < int(config.QUEST_BOARD_MIN_SECOND_PAGE_DELAY_SECONDS)
        ):
            return {"qualified": False, "visitor_token": visitor_token, "clear_attribution": False}
        qualification_type = QuestQualifiedVisit.TYPE_SITE_SECOND_PAGE
        second_page = page
    else:
        if not attribution.get("platform_verified") or page != landing:
            return {"qualified": False, "visitor_token": visitor_token, "clear_attribution": False}
        qualification_type = QuestQualifiedVisit.TYPE_VIDEO_PLATFORM

    try:
        with transaction.atomic():
            visit = QuestQualifiedVisit.objects.create(
                campaign=campaign,
                cycle_key=cycle.key,
                visitor_hash=browser_visitor_hash,
                network_hash=network_hash,
                fingerprint_hash=browser_hash,
                landing_page=landing,
                second_page=second_page,
                referer_host=str(attribution.get("referer_host") or "")[:255],
                qualification_type=qualification_type,
            )
    except IntegrityError:
        return {"qualified": False, "visitor_token": visitor_token, "clear_attribution": True}

    _grant_completed_campaign_quest(campaign)
    return {
        "qualified": True,
        "visit_id": visit.pk,
        "visitor_token": visitor_token,
        "clear_attribution": True,
    }


def _countdown_label(cycle: QuestCycle) -> str:
    remaining = max(0, int((cycle.ends_at - timezone.now()).total_seconds()))
    days, remainder = divmod(remaining, 86400)
    hours, _remainder = divmod(remainder, 3600)
    if days:
        return f"{days}d {hours}h"
    minutes = max(0, remaining % 3600 // 60)
    return f"{hours}h {minutes}m"


def _weekly_row(*, user, cycle: QuestCycle, definition: WeeklyQuestDefinition) -> dict:
    progress = _quest_progress(user=user, cycle=cycle, definition=definition)
    grant = None
    if progress["complete"]:
        grant = _ensure_reward_grant(user=user, cycle=cycle, definition=definition)
    claimed = bool(grant and grant.status == RewardChestGrant.STATUS_OPENED)
    can_claim = bool(grant and grant.status == RewardChestGrant.STATUS_PENDING)

    if claimed:
        button_label = str(config.QUEST_BOARD_WEEKLY_CLAIMED_LABEL)
    elif can_claim:
        button_label = str(config.QUEST_BOARD_WEEKLY_OPEN_CHEST_LABEL)
    else:
        button_label = definition.action_label

    return {
        "empty": False,
        "key": definition.key,
        "title": definition.title,
        "description": definition.description,
        "condition": definition.condition,
        "icon_path": definition.icon_path,
        "icon_material": definition.icon_material,
        "reward_kind": "chest",
        "reward_display": definition.chest_label,
        "reward_image_path": definition.chest_image_path,
        "current": progress["current"],
        "target": progress["target"],
        "progress_text": progress["progress_text"],
        "progress_percent": progress["progress_percent"],
        "complete": progress["complete"],
        "claimed": claimed,
        "status": "claimed" if claimed else "complete" if progress["complete"] else "in_progress",
        "button_label": button_label,
        "can_claim": can_claim,
        "claim_url": reverse(
            "wallet_open_weekly_quest",
            kwargs={"cycle_key": cycle.key, "quest_key": definition.key},
        ),
        "action_url": "" if progress["complete"] else definition.action_url,
        "share_action": "site" if definition.condition == "site_visitors" else "",
    }



def _anonymous_preview_keys(
    *,
    cycle: QuestCycle,
) -> tuple[str, ...]:
    definitions = config.QUEST_BOARD_WEEKLY_QUESTS

    if not isinstance(definitions, dict):
        raise ImproperlyConfigured(
            "QUEST_BOARD_WEEKLY_QUESTS must be a dictionary"
        )

    enabled_keys = []

    for key, raw in definitions.items():
        if not isinstance(raw, dict):
            raise ImproperlyConfigured(
                f"Weekly quest {key} must be a dictionary"
            )

        enabled = raw.get("enabled", True)

        if not isinstance(enabled, bool):
            raise ImproperlyConfigured(
                f"Quest {key}.enabled must be True or False"
            )

        if enabled:
            enabled_keys.append(str(key))

    slot_count = int(config.QUEST_BOARD_SLOT_COUNT)

    if slot_count < 1:
        raise ImproperlyConfigured(
            "QUEST_BOARD_SLOT_COUNT must be positive"
        )

    if not enabled_keys:
        return ()

    selection_salt = str(
        config.QUEST_BOARD_WEEKLY_SELECTION_SALT or ""
    ).strip()

    if not selection_salt:
        raise ImproperlyConfigured(
            "QUEST_BOARD_WEEKLY_SELECTION_SALT cannot be empty"
        )

    seed_prefix = (
        f"{selection_salt}:"
        f"{cycle.key}:"
        "anonymous-preview"
    )

    def rank(quest_key: str):
        return hmac.new(
            settings.SECRET_KEY.encode("utf-8"),
            (
                f"{seed_prefix}:"
                f"quest:{quest_key}"
            ).encode("utf-8"),
            hashlib.sha256,
        ).digest()

    ordered = sorted(
        enabled_keys,
        key=lambda quest_key: (
            rank(quest_key),
            quest_key,
        ),
    )

    return tuple(
        ordered[:slot_count]
    )


def _weekly_preview_row(
    *,
    definition: WeeklyQuestDefinition,
    login_url: str,
) -> dict:
    if definition.condition == "video_share":
        target = definition.target
        progress_text = definition.progress_pending_text

    elif definition.condition.startswith("community_"):
        target = definition.personal_target
        progress_text = _format_progress_text(
            definition=definition,
            template=definition.progress_text,
            values={
                "personal_current": 0,
                "personal_target": definition.personal_target,
                "global_current": 0,
                "global_target": definition.global_target,
            },
        )

    else:
        target = definition.target
        progress_text = _format_progress_text(
            definition=definition,
            template=definition.progress_text,
            values={
                "current": 0,
                "target": definition.target,
            },
        )

    return {
        "empty": False,
        "key": definition.key,
        "title": definition.title,
        "description": definition.description,
        "condition": definition.condition,
        "icon_path": definition.icon_path,
        "icon_material": definition.icon_material,
        "reward_kind": "chest",
        "reward_display": definition.chest_label,
        "reward_image_path": definition.chest_image_path,
        "current": 0,
        "target": target,
        "progress_text": progress_text,
        "progress_percent": 0,
        "complete": False,
        "claimed": False,
        "status": "in_progress",
        "button_label": definition.action_label,
        "can_claim": False,
        "claim_url": login_url,
        "action_url": login_url,
        "share_action": "",
    }


def build_weekly_quest_preview_context(
    *,
    login_url: str,
) -> dict | None:
    if not _weekly_enabled():
        return None

    cycle = _cycle_for_datetime()

    definitions = tuple(
        _definition_from_config(key)
        for key in _anonymous_preview_keys(
            cycle=cycle,
        )
    )

    rows = [
        _weekly_preview_row(
            definition=definition,
            login_url=login_url,
        )
        for definition in definitions
    ]

    slot_count = int(
        config.QUEST_BOARD_SLOT_COUNT
    )

    while len(rows) < slot_count:
        rows.append(
            {
                "empty": True,
                "slot": len(rows) + 1,
            }
        )

    revision_material = [
        "anonymous-preview",
        cycle.key,
        str(config.QUEST_BOARD_CONFIG_VERSION),
        *[
            definition.key
            for definition in definitions
        ],
    ]

    revision = hashlib.sha256(
        "|".join(
            revision_material
        ).encode("utf-8")
    ).hexdigest()[:16]

    return {
        "enabled": True,
        "config_version": int(
            config.QUEST_BOARD_CONFIG_VERSION
        ),
        "slot_count": slot_count,
        "active_count": len(definitions),
        "completed_count": 0,
        "claimed_count": 0,
        "title": str(
            config.QUEST_BOARD_WEEKLY_TITLE
        ),
        "subtitle": str(
            config.QUEST_BOARD_WEEKLY_SUBTITLE
        ),
        "reset_prefix": str(
            config.QUEST_BOARD_WEEKLY_RESET_PREFIX
        ),
        "reset_label": _countdown_label(cycle),
        "show_schedule": True,
        "slots": rows,
        "cycle_key": cycle.key,
        "ends_at_iso": cycle.ends_at.isoformat(),
        "revision": revision,

        # No status polling and no share-link creation
        # while the visitor is anonymous.
        "status_url": "",
        "site_link_url": "",

        "preview": True,
    }


def build_weekly_quest_board_context(*, user) -> dict:
    user = _require_user(user)
    cycle = _cycle_for_datetime()
    starter = build_starter_quest_board_context(user=user)
    if not _weekly_enabled():
        return starter
    starter_rows = [
        row
        for row in starter.get("slots", ())
        if not row.get("empty") and not row.get("claimed")
    ][:1]
    weekly_rows = [
        _weekly_row(user=user, cycle=cycle, definition=definition)
        for definition in get_weekly_definitions(user=user, cycle=cycle)
    ]
    slot_count = int(config.QUEST_BOARD_SLOT_COUNT)
    rows = starter_rows + weekly_rows[: max(0, slot_count - len(starter_rows))]
    while len(rows) < slot_count:
        rows.append({"empty": True, "slot": len(rows) + 1})

    revision_material = [
        cycle.key,
        str(config.QUEST_BOARD_CONFIG_VERSION),
        *[
            f"{row.get('key')}:{row.get('current')}:{int(bool(row.get('complete')))}:{int(bool(row.get('claimed')))}"
            for row in rows
            if not row.get("empty")
        ],
    ]
    revision = hashlib.sha256("|".join(revision_material).encode("utf-8")).hexdigest()[:16]
    return {
        "enabled": bool(config.QUEST_BOARD_ENABLED),
        "config_version": int(config.QUEST_BOARD_CONFIG_VERSION),
        "slot_count": slot_count,
        "active_count": sum(1 for row in rows if not row.get("empty")),
        "completed_count": sum(1 for row in rows if row.get("complete")),
        "claimed_count": sum(1 for row in rows if row.get("claimed")),
        "title": str(config.QUEST_BOARD_WEEKLY_TITLE),
        "subtitle": str(config.QUEST_BOARD_WEEKLY_SUBTITLE),
        "reset_prefix": str(config.QUEST_BOARD_WEEKLY_RESET_PREFIX),
        "reset_label": _countdown_label(cycle),
        "show_schedule": True,
        "slots": rows,
        "cycle_key": cycle.key,
        "ends_at_iso": cycle.ends_at.isoformat(),
        "revision": revision,
        "status_url": reverse("weekly_quest_status"),
        "site_link_url": reverse("weekly_quest_site_link"),
    }


def build_weekly_quest_status(*, user) -> dict:
    context = build_weekly_quest_board_context(user=user)
    return {
        "cycle_key": context["cycle_key"],
        "revision": context["revision"],
        "ends_at": context["ends_at_iso"],
    }


@transaction.atomic
def prepare_weekly_quest_reward(
    *,
    user,
    cycle_key: str,
    quest_key: str,
) -> dict:
    user = _require_user(user)
    cycle = _cycle_from_key(cycle_key)
    definition = next(
        (
            row
            for row in get_weekly_definitions(user=user, cycle=cycle)
            if row.key == quest_key
        ),
        None,
    )
    if definition is None:
        raise ValidationError("Quest is not active in this cycle")

    progress = _quest_progress(
        user=user,
        cycle=cycle,
        definition=definition,
    )
    if not progress["complete"]:
        raise ValidationError("Quest requirements are not complete")

    grant = _ensure_reward_grant(
        user=user,
        cycle=cycle,
        definition=definition,
    )
    snapshot = config.reward_chest_definition_from_snapshot(
        grant.config_snapshot
    )
    return {
        "prepared": True,
        "grant": grant,
        "chest_name": snapshot.label,
        "closed_image_path": snapshot.closed_image,
        "opened_image_path": snapshot.opened_image,
        "box_state": "closed",
    }


def open_weekly_quest_reward(
    *,
    user,
    cycle_key: str,
    quest_key: str,
    grant_public_id=None,
) -> dict:
    prepared = prepare_weekly_quest_reward(
        user=user,
        cycle_key=cycle_key,
        quest_key=quest_key,
    )
    grant = prepared["grant"]

    if (
        grant_public_id not in (None, "")
        and str(grant.public_id) != str(grant_public_id)
    ):
        raise ValidationError(
            "The prepared weekly quest chest does not match"
        )

    return open_reward_chest(user=user, grant=grant)
