from __future__ import annotations

import contextlib
import json
import secrets
import threading
import uuid
from decimal import Decimal

from django.conf import settings
from django.core import signing
from django_redis import get_redis_connection

from .models import AdCampaign, AdCreative

TOKEN_SCALE = 10 ** 6
NANOS_PER_MICROTOKEN = 1000
TOKEN_NANOS = TOKEN_SCALE * NANOS_PER_MICROTOKEN
PREFIX = "ads:v1"
BANNER_SLOTS = {
    AdCampaign.PLACEMENT_HOME,
    AdCampaign.PLACEMENT_SIDEBAR,
}
RESERVATION_SLOTS = {
    AdCampaign.PLACEMENT_PREROLL,
    AdCampaign.PLACEMENT_MIDROLL,
    AdCampaign.PLACEMENT_POSTROLL,
    AdCampaign.PLACEMENT_POPUNDER,
}
DIRECT_SLOTS = tuple(value for value, _label in AdCampaign.PLACEMENT_CHOICES)

_local = threading.local()


def redis_connection():
    return get_redis_connection("default")


def wallet_funded_key(user_id):
    return f"{PREFIX}:account:{int(user_id)}:funded_nanos"


def account_accrued_key(user_id):
    return f"{PREFIX}:account:{int(user_id)}:accrued_nanos"


def campaign_accrued_key(campaign_id):
    return f"{PREFIX}:campaign:{int(campaign_id)}:accrued_nanos"


def campaign_impressions_key(campaign_id):
    return f"{PREFIX}:campaign:{int(campaign_id)}:pending_impressions"


def campaign_clicks_key(campaign_id):
    return f"{PREFIX}:campaign:{int(campaign_id)}:pending_clicks"


def campaign_config_key(campaign_id):
    return f"{PREFIX}:campaign:{int(campaign_id)}:config"


def slot_key(slot):
    return f"{PREFIX}:slot:{slot}"


def pause_queue_key():
    return f"{PREFIX}:pause_funds"


def account_sync_lock_name(user_id):
    return f"{PREFIX}:account:{int(user_id)}:sync_lock"


def settlement_lock_name(campaign_id):
    return f"{PREFIX}:campaign:{int(campaign_id)}:settlement_lock"


def wallet_sync_suppressed():
    return bool(getattr(_local, "suppress_wallet_sync", False))


@contextlib.contextmanager
def suppress_wallet_runtime_sync():
    previous = wallet_sync_suppressed()
    _local.suppress_wallet_sync = True
    try:
        yield
    finally:
        _local.suppress_wallet_sync = previous


def acquire_account_sync_lock(user_id, *, blocking_timeout=3):
    return redis_connection().lock(
        account_sync_lock_name(user_id),
        timeout=20,
        blocking_timeout=blocking_timeout,
    )


def microtokens_to_nanos(value):
    return int(value) * NANOS_PER_MICROTOKEN


def nanos_to_token_decimal(value):
    return Decimal(int(value)) / Decimal(TOKEN_NANOS)


def get_account_unsettled_nanos(user_id):
    raw = redis_connection().get(account_accrued_key(user_id))
    return int(raw or 0)


def get_account_unsettled_microtokens(user_id):
    # Round UP, not down: other wallet outflows must not spend any fraction that
    # has already been consumed by Ads.
    nanos = max(0, get_account_unsettled_nanos(user_id))
    return (nanos + NANOS_PER_MICROTOKEN - 1) // NANOS_PER_MICROTOKEN


def get_effective_balance_nanos(user_id):
    redis = redis_connection()
    values = redis.mget(wallet_funded_key(user_id), account_accrued_key(user_id))
    funded = int(values[0] or 0)
    accrued = int(values[1] or 0)
    return max(0, funded - accrued)


def sync_wallet_runtime(wallet):
    if wallet.user_id is None:
        return
    redis = redis_connection()
    lock = acquire_account_sync_lock(wallet.user_id)
    if not lock.acquire(blocking=True):
        return
    try:
        funded_microtokens = max(0, int(wallet.balance) - int(wallet.held_balance))
        redis.set(
            wallet_funded_key(wallet.user_id),
            microtokens_to_nanos(funded_microtokens),
        )
    finally:
        try:
            lock.release()
        except Exception:
            pass


def event_cost_nanos(pricing_model, bid_microtokens):
    bid_microtokens = int(bid_microtokens)
    if pricing_model == AdCampaign.PRICING_CPM:
        # bid_microtokens / 1000 impressions -> bid_microtokens nano-tokens
        # per impression because 1 micro-token = 1000 nano-tokens.
        return bid_microtokens
    return bid_microtokens * NANOS_PER_MICROTOKEN


def predicted_ctr_ppm(*, impressions, clicks):
    prior_impressions = int(getattr(settings, "ADS_CPC_PRIOR_IMPRESSIONS", 1000))
    prior_ctr_ppm = int(getattr(settings, "ADS_CPC_PRIOR_CTR_PPM", 10000))
    impressions = max(0, int(impressions))
    clicks = max(0, int(clicks))
    denominator = impressions + prior_impressions
    if denominator <= 0:
        return prior_ctr_ppm
    numerator = clicks * 1_000_000 + prior_impressions * prior_ctr_ppm
    return max(1, numerator // denominator)


def campaign_ecpm_microtokens(campaign, *, live_impressions=0, live_clicks=0):
    if campaign.pricing_model == AdCampaign.PRICING_CPM:
        return int(campaign.bid_microtokens)
    if campaign.placement == AdCampaign.PLACEMENT_POPUNDER:
        # A successfully opened popunder is one impression and one click.
        return max(1, int(campaign.bid_microtokens) * 1000)
    impressions = int(campaign.impressions) + int(live_impressions)
    clicks = int(campaign.clicks) + int(live_clicks)
    ctr_ppm = predicted_ctr_ppm(impressions=impressions, clicks=clicks)
    return max(1, int(campaign.bid_microtokens) * ctr_ppm * 1000 // 1_000_000)


def _redis_int(redis, key):
    value = redis.get(key)
    return int(value or 0)


def _remove_campaign_from_slots(redis, campaign_id):
    for slot in DIRECT_SLOTS:
        redis.zrem(slot_key(slot), campaign_id)


def sync_campaign_runtime(campaign):
    redis = redis_connection()
    cfg_key = campaign_config_key(campaign.pk)
    advertiser_allowed = bool(
        getattr(campaign.advertiser, "advertiserUser", False)
        or getattr(campaign.advertiser, "is_superuser", False)
    )

    if (
        not advertiser_allowed
        or campaign.review_status != AdCampaign.REVIEW_APPROVED
        or campaign.delivery_status != AdCampaign.DELIVERY_ACTIVE
    ):
        redis.delete(cfg_key)
        _remove_campaign_from_slots(redis, campaign.pk)
        return

    links = (
        campaign.creative_links
        .filter(
            enabled=True,
            creative__review_status=AdCreative.REVIEW_APPROVED,
            creative__placement=campaign.creative_format,
            creative__advertiser_id=campaign.advertiser_id,
        )
        .select_related("creative")
        .order_by("id")
    )
    creative_pool = []
    for link in links:
        creative = link.creative
        try:
            banner_url = creative.image.url if creative.image else ""
        except Exception:
            banner_url = ""

        item = {
            "id": int(link.creative_id),
            "format": creative.placement,
            "url": banner_url,
            "vast_url": str(creative.vast_url or ""),
            "destination_url": str(creative.destination_url or ""),
            "weight": max(1, int(link.weight or 1)),
        }

        usable = False
        if creative.is_banner:
            usable = bool(banner_url)
        elif creative.is_in_video:
            usable = bool(creative.vast_url)
        elif creative.is_popunder:
            usable = bool(creative.destination_url)

        if usable:
            creative_pool.append(item)

    if not creative_pool:
        redis.delete(cfg_key)
        _remove_campaign_from_slots(redis, campaign.pk)
        return

    live_impressions = _redis_int(
        redis,
        campaign_impressions_key(campaign.pk),
    )
    live_clicks = _redis_int(
        redis,
        campaign_clicks_key(campaign.pk),
    )
    score = campaign_ecpm_microtokens(
        campaign,
        live_impressions=live_impressions,
        live_clicks=live_clicks,
    )
    effective = get_effective_balance_nanos(
        campaign.advertiser_id
    )
    funded = int(
        effective
        >= event_cost_nanos(
            campaign.pricing_model,
            campaign.bid_microtokens,
        )
    )

    mapping = {
        "campaign_id": campaign.pk,
        "advertiser_id": campaign.advertiser_id,
        "slot": campaign.placement,
        "pricing": campaign.pricing_model,
        "bid_microtokens": int(campaign.bid_microtokens),
        "creative_pool": json.dumps(
            creative_pool,
            separators=(",", ":"),
        ),
        "target_url": campaign.target_url,
        "funded": funded,
        "ecpm_microtokens": score,
    }
    redis.hset(cfg_key, mapping=mapping)
    redis.expire(cfg_key, 120)

    _remove_campaign_from_slots(redis, campaign.pk)
    if funded:
        redis.zadd(
            slot_key(campaign.placement),
            {str(campaign.pk): score},
        )


IMPRESSION_LUA = r"""
local once_key = KEYS[1]
local cfg = KEYS[2]
local slotz = KEYS[3]
local funded_key = KEYS[4]
local account_accrued = KEYS[5]
local campaign_accrued = KEYS[6]
local impressions = KEYS[7]
local pause_queue = KEYS[8]

local campaign_id = ARGV[1]
local pricing = ARGV[2]
local event_cost = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

if redis.call('EXISTS', cfg) == 0 then
  redis.call('ZREM', slotz, campaign_id)
  return 0
end

if redis.call('HGET', cfg, 'funded') ~= '1' then
  redis.call('ZREM', slotz, campaign_id)
  return 0
end

local funded = tonumber(redis.call('GET', funded_key) or '0')
local accrued = tonumber(redis.call('GET', account_accrued) or '0')
if (funded - accrued) < event_cost then
  redis.call('HSET', cfg, 'funded', '0')
  redis.call('ZREM', slotz, campaign_id)
  redis.call('SADD', pause_queue, campaign_id)
  return -1
end

if redis.call('SET', once_key, '1', 'NX', 'EX', ttl) == false then
  return 2
end

redis.call('INCR', impressions)
if pricing == 'cpm' then
  redis.call('INCRBY', account_accrued, event_cost)
  redis.call('INCRBY', campaign_accrued, event_cost)
end
return 1
"""


CLICK_LUA = r"""
local once_key = KEYS[1]
local impression_once_key = KEYS[2]
local cfg = KEYS[3]
local slotz = KEYS[4]
local funded_key = KEYS[5]
local account_accrued = KEYS[6]
local campaign_accrued = KEYS[7]
local clicks = KEYS[8]
local pause_queue = KEYS[9]

local campaign_id = ARGV[1]
local pricing = ARGV[2]
local click_cost = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

if redis.call('EXISTS', impression_once_key) == 0 then
  return 0
end

if redis.call('SET', once_key, '1', 'NX', 'EX', ttl) == false then
  return 2
end

redis.call('INCR', clicks)

if pricing ~= 'cpc' then
  return 1
end

local funded = tonumber(redis.call('GET', funded_key) or '0')
local accrued = tonumber(redis.call('GET', account_accrued) or '0')
if (funded - accrued) < click_cost then
  if redis.call('EXISTS', cfg) == 1 then
    redis.call('HSET', cfg, 'funded', '0')
  end
  redis.call('ZREM', slotz, campaign_id)
  redis.call('SADD', pause_queue, campaign_id)
  return -1
end

redis.call('INCRBY', account_accrued, click_cost)
redis.call('INCRBY', campaign_accrued, click_cost)
return 1
"""


ACK_SETTLEMENT_LUA = r"""
local funded_key = KEYS[1]
local account_accrued = KEYS[2]
local campaign_accrued = KEYS[3]
local impressions = KEYS[4]
local clicks = KEYS[5]

local settled_nanos = tonumber(ARGV[1])
local settled_impressions = tonumber(ARGV[2])
local settled_clicks = tonumber(ARGV[3])

if settled_nanos > 0 then
  local funded = tonumber(redis.call('GET', funded_key) or '0')
  local account_pending = tonumber(redis.call('GET', account_accrued) or '0')
  local campaign_pending = tonumber(redis.call('GET', campaign_accrued) or '0')
  redis.call('SET', funded_key, math.max(0, funded - settled_nanos))
  redis.call('SET', account_accrued, math.max(0, account_pending - settled_nanos))
  redis.call('SET', campaign_accrued, math.max(0, campaign_pending - settled_nanos))
end

if settled_impressions > 0 then
  local value = tonumber(redis.call('GET', impressions) or '0')
  redis.call('SET', impressions, math.max(0, value - settled_impressions))
end
if settled_clicks > 0 then
  local value = tonumber(redis.call('GET', clicks) or '0')
  redis.call('SET', clicks, math.max(0, value - settled_clicks))
end
return 1
"""


def _decode_hash(raw):
    return {
        (k.decode() if isinstance(k, bytes) else str(k)): (
            v.decode() if isinstance(v, bytes) else str(v)
        )
        for k, v in raw.items()
    }


def _choose_creative(creative_pool):
    normalized = []
    total_weight = 0
    for item in creative_pool or []:
        try:
            creative_id = int(item["id"])
            weight = max(1, int(item.get("weight", 1)))
        except (KeyError, TypeError, ValueError):
            continue

        banner_url = str(item.get("url") or "")
        vast_url = str(item.get("vast_url") or "")
        destination_url = str(item.get("destination_url") or "")
        creative_format = str(item.get("format") or "")

        if not any((banner_url, vast_url, destination_url)):
            continue

        normalized.append(
            {
                "id": creative_id,
                "format": creative_format,
                "url": banner_url,
                "vast_url": vast_url,
                "destination_url": destination_url,
                "weight": weight,
            }
        )
        total_weight += weight

    if not normalized or total_weight <= 0:
        return None

    pick = secrets.randbelow(total_weight)
    cursor = 0
    for item in normalized:
        cursor += item["weight"]
        if pick < cursor:
            return item
    return normalized[-1]


def _candidate_payload(slot, *, bill_impression):
    if slot not in DIRECT_SLOTS:
        return None

    redis = redis_connection()
    seen_campaign_ids = set()

    while True:
        candidates = redis.zrevrange(
            slot_key(slot),
            0,
            19,
        )
        candidates = [
            raw_id
            for raw_id in candidates
            if int(raw_id) not in seen_campaign_ids
        ]
        if not candidates:
            return None

        for raw_id in candidates:
            campaign_id = int(raw_id)
            seen_campaign_ids.add(campaign_id)
            cfg = _decode_hash(
                redis.hgetall(campaign_config_key(campaign_id))
            )
            if (
                not cfg
                or cfg.get("slot") != slot
                or cfg.get("funded") != "1"
            ):
                redis.zrem(slot_key(slot), campaign_id)
                continue

            try:
                creative_pool = json.loads(
                    cfg.get("creative_pool") or "[]"
                )
            except (TypeError, ValueError):
                creative_pool = []

            creative = _choose_creative(creative_pool)
            if creative is None:
                redis.zrem(slot_key(slot), campaign_id)
                continue

            advertiser_id = int(cfg["advertiser_id"])
            pricing = cfg["pricing"]
            bid_microtokens = int(cfg["bid_microtokens"])
            impression_id = uuid.uuid4().hex

            if creative["format"] in {
                AdCreative.PLACEMENT_HOME,
                AdCreative.PLACEMENT_SIDEBAR,
            }:
                target_url = str(cfg.get("target_url") or "")
            elif creative["format"] == AdCreative.PLACEMENT_POPUNDER:
                target_url = creative["destination_url"]
            else:
                target_url = ""

            payload = {
                "c": campaign_id,
                "r": creative["id"],
                "a": advertiser_id,
                "p": pricing,
                "b": bid_microtokens,
                "s": slot,
                "i": impression_id,
                "u": target_url,
            }

            if bill_impression:
                result = record_impression(payload)
                if result != 1:
                    continue

            event_token = signing.dumps(
                payload,
                salt="ads.click.v1",
                compress=True,
            )
            click_url = (
                f"/ads/click/{event_token}/"
                if target_url
                else ""
            )

            return {
                "campaign_id": campaign_id,
                "creative_id": creative["id"],
                "creative_format": creative["format"],
                "creative_url": creative["url"],
                "vast_url": creative["vast_url"],
                "destination_url": target_url,
                "click_url": click_url,
                "event_token": event_token,
                "pricing_model": pricing,
            }


def serve(slot):
    if slot not in BANNER_SLOTS:
        return None
    return _candidate_payload(slot, bill_impression=True)


def reserve(slot):
    if slot not in RESERVATION_SLOTS:
        return None
    return _candidate_payload(slot, bill_impression=False)


def record_impression(payload):
    redis = redis_connection()
    campaign_id = int(payload["c"])
    advertiser_id = int(payload["a"])
    pricing = str(payload["p"])
    bid_microtokens = int(payload["b"])
    slot = str(payload["s"])
    impression_id = str(payload["i"])
    affordability_cost = event_cost_nanos(
        pricing,
        bid_microtokens,
    )
    ttl = int(
        getattr(
            settings,
            "ADS_CLICK_TOKEN_MAX_AGE_SECONDS",
            7 * 24 * 60 * 60,
        )
    )
    impression_script = redis.register_script(IMPRESSION_LUA)
    return int(
        impression_script(
            keys=[
                f"{PREFIX}:impression-once:{impression_id}",
                campaign_config_key(campaign_id),
                slot_key(slot),
                wallet_funded_key(advertiser_id),
                account_accrued_key(advertiser_id),
                campaign_accrued_key(campaign_id),
                campaign_impressions_key(campaign_id),
                pause_queue_key(),
            ],
            args=[
                campaign_id,
                pricing,
                affordability_cost,
                ttl,
            ],
        )
    )


def record_click(payload):
    redis = redis_connection()
    campaign_id = int(payload["c"])
    advertiser_id = int(payload["a"])
    pricing = str(payload["p"])
    bid_microtokens = int(payload["b"])
    slot = str(payload["s"])
    impression_id = str(payload["i"])
    click_cost = (
        event_cost_nanos(pricing, bid_microtokens)
        if pricing == AdCampaign.PRICING_CPC
        else 0
    )
    ttl = int(getattr(settings, "ADS_CLICK_TOKEN_MAX_AGE_SECONDS", 7 * 24 * 60 * 60))
    click_script = redis.register_script(CLICK_LUA)
    return int(
        click_script(
            keys=[
                f"{PREFIX}:click-once:{impression_id}",
                f"{PREFIX}:impression-once:{impression_id}",
                campaign_config_key(campaign_id),
                slot_key(slot),
                wallet_funded_key(advertiser_id),
                account_accrued_key(advertiser_id),
                campaign_accrued_key(campaign_id),
                campaign_clicks_key(campaign_id),
                pause_queue_key(),
            ],
            args=[
                campaign_id,
                pricing,
                click_cost,
                ttl,
            ],
        )
    )


def get_campaign_live_metrics(campaign):
    redis = redis_connection()
    values = redis.mget(
        campaign_impressions_key(campaign.pk),
        campaign_clicks_key(campaign.pk),
        campaign_accrued_key(campaign.pk),
    )
    pending_impressions = int(values[0] or 0)
    pending_clicks = int(values[1] or 0)
    pending_spend_nanos = int(values[2] or 0)
    impressions = int(campaign.impressions) + pending_impressions
    clicks = int(campaign.clicks) + pending_clicks
    spend_nanos = int(campaign.spend_microtokens) * NANOS_PER_MICROTOKEN + pending_spend_nanos
    return {
        "impressions": impressions,
        "clicks": clicks,
        "spend_nanos": spend_nanos,
        "pending_impressions": pending_impressions,
        "pending_clicks": pending_clicks,
        "pending_spend_nanos": pending_spend_nanos,
        "ctr": (Decimal(clicks) * Decimal(100) / Decimal(impressions)) if impressions else Decimal(0),
    }


def ack_settlement(*, batch):
    redis = redis_connection()
    script = redis.register_script(ACK_SETTLEMENT_LUA)
    settled_nanos = int(batch.amount_microtokens) * NANOS_PER_MICROTOKEN
    script(
        keys=[
            wallet_funded_key(batch.advertiser_id),
            account_accrued_key(batch.advertiser_id),
            campaign_accrued_key(batch.campaign_id),
            campaign_impressions_key(batch.campaign_id),
            campaign_clicks_key(batch.campaign_id),
        ],
        args=[
            settled_nanos,
            int(batch.impressions),
            int(batch.clicks),
        ],
    )


def drop_campaign_runtime(campaign_id):
    redis = redis_connection()
    redis.delete(campaign_config_key(campaign_id))
    _remove_campaign_from_slots(redis, campaign_id)
