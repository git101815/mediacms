import itertools
import uuid
from io import BytesIO

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from PIL import Image

import ads.runtime as runtime
from ads.models import AdCampaign, AdCampaignCreative, AdCreative
from ledger.models import TokenWallet


_counter = itertools.count(1)


def make_banner_file(name="banner.png", size=(728, 90), fmt="PNG"):
    buffer = BytesIO()
    image = Image.new("RGB", size)
    if fmt == "GIF":
        image.save(buffer, format="GIF")
        content_type = "image/gif"
    elif fmt == "JPEG":
        image.save(buffer, format="JPEG")
        content_type = "image/jpeg"
    elif fmt == "WEBP":
        image.save(buffer, format="WEBP")
        content_type = "image/webp"
    else:
        image.save(buffer, format="PNG")
        content_type = "image/png"
    return SimpleUploadedFile(
        name,
        buffer.getvalue(),
        content_type=content_type,
    )


@pytest.fixture(autouse=True)
def _isolated_ads_media_root(settings, tmp_path):
    settings.MEDIA_ROOT = str(tmp_path / "media")


@pytest.fixture
def ads_redis(monkeypatch):
    prefix = f"ads:test:{uuid.uuid4().hex}"
    monkeypatch.setattr(runtime, "PREFIX", prefix)
    redis = runtime.redis_connection()
    try:
        redis.ping()
    except Exception as exc:
        pytest.fail(f"Redis is required for Ads integration tests: {exc}")

    yield redis

    keys = list(redis.scan_iter(f"{prefix}:*"))
    if keys:
        redis.delete(*keys)


@pytest.fixture
def advertiser_factory(django_user_model, ads_redis):
    def create(
        *,
        username=None,
        balance=1_000_000_000,
        held_balance=0,
        advertiser=True,
        superuser=False,
    ):
        suffix = next(_counter)
        username = username or f"ads-user-{suffix}"
        user = django_user_model.objects.create_user(
            username=username,
            password="test-password-123",
            advertiserUser=advertiser,
            is_superuser=superuser,
            is_staff=superuser,
        )
        wallet, _ = TokenWallet.objects.get_or_create(
            user=user,
            defaults={
                "wallet_type": TokenWallet.TYPE_USER,
                "allow_negative": False,
            },
        )
        wallet.balance = int(balance)
        wallet.held_balance = int(held_balance)
        wallet.save(
            update_fields=[
                "balance",
                "held_balance",
                "updated_at",
            ]
        )
        runtime.sync_wallet_runtime(wallet)
        return user

    return create


@pytest.fixture
def creative_factory():
    def create(
        *,
        advertiser,
        placement=AdCreative.PLACEMENT_HOME,
        approved=True,
        name=None,
        review_status=None,
        image=None,
        vast_url="https://ads.example/vast.xml",
        destination_url="https://example.com/landing",
    ):
        suffix = next(_counter)
        if review_status is None:
            review_status = (
                AdCreative.REVIEW_APPROVED
                if approved
                else AdCreative.REVIEW_PENDING
            )

        kwargs = {
            "advertiser": advertiser,
            "name": name or f"Creative {suffix}",
            "placement": placement,
            "review_status": review_status,
        }
        if placement == AdCreative.PLACEMENT_HOME:
            kwargs["image"] = image or make_banner_file(
                f"home-{suffix}.png",
                (728, 90),
            )
        elif placement == AdCreative.PLACEMENT_SIDEBAR:
            kwargs["image"] = image or make_banner_file(
                f"sidebar-{suffix}.png",
                (300, 250),
            )
        elif placement == AdCreative.PLACEMENT_IN_VIDEO:
            kwargs["vast_url"] = vast_url
        elif placement == AdCreative.PLACEMENT_POPUNDER:
            kwargs["destination_url"] = destination_url

        return AdCreative.objects.create(**kwargs)

    return create


@pytest.fixture
def campaign_factory(creative_factory):
    def create(
        *,
        advertiser,
        placement=AdCampaign.PLACEMENT_HOME,
        pricing=AdCampaign.PRICING_CPM,
        bid_microtokens=1_000_000,
        approved=True,
        review_status=None,
        delivery_status=AdCampaign.DELIVERY_ACTIVE,
        creative=None,
        with_creative=True,
        target_url="https://example.com/click",
        name=None,
    ):
        suffix = next(_counter)
        if review_status is None:
            review_status = (
                AdCampaign.REVIEW_APPROVED
                if approved
                else AdCampaign.REVIEW_PENDING
            )
        campaign = AdCampaign.objects.create(
            advertiser=advertiser,
            name=name or f"Campaign {suffix}",
            placement=placement,
            target_url=(
                target_url
                if placement
                in {
                    AdCampaign.PLACEMENT_HOME,
                    AdCampaign.PLACEMENT_SIDEBAR,
                }
                else ""
            ),
            pricing_model=pricing,
            bid_microtokens=int(bid_microtokens),
            review_status=review_status,
            delivery_status=delivery_status,
        )
        if with_creative:
            if creative is None:
                if placement in {
                    AdCampaign.PLACEMENT_PREROLL,
                    AdCampaign.PLACEMENT_MIDROLL,
                    AdCampaign.PLACEMENT_POSTROLL,
                }:
                    creative_placement = AdCreative.PLACEMENT_IN_VIDEO
                else:
                    creative_placement = placement
                creative = creative_factory(
                    advertiser=advertiser,
                    placement=creative_placement,
                )
            AdCampaignCreative.objects.create(
                campaign=campaign,
                creative=creative,
                enabled=True,
                weight=1,
            )
        return campaign

    return create
