from unittest.mock import patch

import pytest
from django.core.exceptions import PermissionDenied, ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import RequestFactory
from django.urls import reverse
from django.utils import timezone

from files.models import Media
from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet
from premium.models import MediaPurchase, PremiumMediaAsset, PremiumMediaUnlock
from premium.services import (
    build_premium_media_state,
    build_premium_playback_payload,
    format_token_amount,
    purchase_premium_media_with_tokens,
    replace_creator_premium_asset_file,
    update_creator_premium_asset_settings,
    user_can_access_premium_media,
    user_can_manage_premium_media,
)
from premium.views import build_premium_watch_url


PRICE_TOKENS = 500 * 10**6


def create_test_media(*, user, friendly_token="premiumtest", title="Premium video"):
    Media.objects.bulk_create(
        [
            Media(
                user=user,
                friendly_token=friendly_token,
                title=title,
                media_file=f"tests/premium/{friendly_token}.mp4",
                media_type="video",
                state="public",
                encoding_status="success",
                is_reviewed=True,
                listable=True,
            )
        ]
    )
    return Media.objects.get(friendly_token=friendly_token)


def create_ready_asset(*, media, price_tokens=PRICE_TOKENS, direct_url="https://example.com/full.mp4"):
    return PremiumMediaAsset.objects.create(
        media=media,
        status=PremiumMediaAsset.STATUS_READY,
        price_tokens=price_tokens,
        storage_backend=PremiumMediaAsset.STORAGE_DIRECT_URL,
        playback_format=PremiumMediaAsset.PLAYBACK_MP4,
        direct_url=direct_url,
        file_name="full.mp4",
        content_type="video/mp4",
        codec="h264",
        video_height=1080,
        size_bytes=12345,
        premium_published_at=timezone.now(),
    )


def create_draft_asset(*, media, price_tokens=PRICE_TOKENS):
    return PremiumMediaAsset.objects.create(
        media=media,
        status=PremiumMediaAsset.STATUS_DRAFT,
        price_tokens=price_tokens,
        storage_backend=PremiumMediaAsset.STORAGE_DIRECT_URL,
        playback_format=PremiumMediaAsset.PLAYBACK_MP4,
        direct_url="https://example.com/draft.mp4",
        file_name="draft.mp4",
        content_type="video/mp4",
        codec="h264",
        video_height=1080,
        size_bytes=12345,
    )


def fund_user_wallet(user, amount):
    wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_USER,
        user=user,
        defaults={"allow_negative": False},
    )
    wallet.balance = amount
    wallet.held_balance = 0
    wallet.save(update_fields=["balance", "held_balance", "updated_at"])
    return wallet


@pytest.mark.django_db
def test_draft_premium_asset_is_not_publicly_available(django_user_model):
    creator = django_user_model.objects.create_user(username="creator_draft")
    viewer = django_user_model.objects.create_user(username="viewer_draft")
    media = create_test_media(user=creator, friendly_token="draftpremium")
    create_draft_asset(media=media)

    request = RequestFactory().get("/view?m=draftpremium")
    request.user = viewer

    state = build_premium_media_state(user=viewer, media=media, request=request)

    assert state["enabled"] is False
    assert state["viewer_has_unlock"] is False
    assert state["purchase_url"] == ""
    assert state["premium_playback_url"] == ""

    with pytest.raises(ValidationError, match="Premium media is not unlocked"):
        build_premium_playback_payload(user=viewer, media=media, request=request)


@pytest.mark.django_db
def test_ready_premium_asset_exposes_purchase_state_but_not_playback_until_unlock(django_user_model):
    creator = django_user_model.objects.create_user(username="creator_ready")
    viewer = django_user_model.objects.create_user(username="viewer_ready")
    media = create_test_media(user=creator, friendly_token="readypremium")
    create_ready_asset(media=media)

    request = RequestFactory().get("/view?m=readypremium")
    request.user = viewer

    state = build_premium_media_state(user=viewer, media=media, request=request)

    assert state["enabled"] is True
    assert state["viewer_has_unlock"] is False
    assert state["price_tokens"] == PRICE_TOKENS
    assert state["price_display"] == "500"
    assert state["purchase_url"].endswith("/api/v1/media/readypremium/purchase")
    assert state["premium_playback_url"] == ""


@pytest.mark.django_db
def test_creator_owner_has_implicit_premium_access_and_manage_permission(django_user_model):
    creator = django_user_model.objects.create_user(username="creator_owner")
    media = create_test_media(user=creator, friendly_token="ownerpremium")
    create_ready_asset(media=media)

    request = RequestFactory().get("/view?m=ownerpremium")
    request.user = creator

    state = build_premium_media_state(user=creator, media=media, request=request)
    payload = build_premium_playback_payload(user=creator, media=media, request=request)

    assert user_can_manage_premium_media(user=creator, media=media) is True
    assert user_can_access_premium_media(user=creator, media=media) is True
    assert state["viewer_has_unlock"] is True
    assert state["premium_playback_url"].endswith("/api/v1/media/ownerpremium/premium-playback")
    assert payload["playback_type"] == "premium"
    assert payload["encodings_info"]["1080"]["h264"]["url"] == "https://example.com/full.mp4"


@pytest.mark.django_db
def test_non_owner_cannot_manage_premium_media_or_upload_file(django_user_model):
    creator = django_user_model.objects.create_user(username="creator_perm")
    other_creator = django_user_model.objects.create_user(username="other_creator_perm")
    media = create_test_media(user=creator, friendly_token="permissionpremium")

    uploaded_file = SimpleUploadedFile(
        "full.mp4",
        b"premium-bytes",
        content_type="video/mp4",
    )

    assert user_can_manage_premium_media(user=other_creator, media=media) is False

    with pytest.raises(ValidationError, match="You cannot manage this premium media"):
        replace_creator_premium_asset_file(
            actor=other_creator,
            media=media,
            uploaded_file=uploaded_file,
        )


@pytest.mark.django_db
def test_replace_premium_asset_file_creates_draft_s3_asset_without_changing_public_media(django_user_model):
    creator = django_user_model.objects.create_user(username="creator_upload")
    media = create_test_media(user=creator, friendly_token="uploadpremium")
    original_media_file = media.media_file.name

    uploaded_file = SimpleUploadedFile(
        "full.mp4",
        b"premium-bytes",
        content_type="video/mp4",
    )

    with patch(
        "premium.services.upload_premium_file_to_private_s3",
        return_value={
            "storage_bucket": "premium-private",
            "storage_key": "premium-media/users/1/media/uploadpremium/mock.mp4",
            "content_type": "video/mp4",
            "file_name": "full.mp4",
            "size_bytes": len(b"premium-bytes"),
        },
    ) as upload_mock:
        asset = replace_creator_premium_asset_file(
            actor=creator,
            media=media,
            uploaded_file=uploaded_file,
        )

    media.refresh_from_db()

    upload_mock.assert_called_once()
    assert media.media_file.name == original_media_file
    assert asset.media == media
    assert asset.status == PremiumMediaAsset.STATUS_DRAFT
    assert asset.storage_backend == PremiumMediaAsset.STORAGE_S3
    assert asset.storage_bucket == "premium-private"
    assert asset.storage_key == "premium-media/users/1/media/uploadpremium/mock.mp4"
    assert asset.price_tokens == 500 * 10**6
    assert asset.file_name == "full.mp4"


@pytest.mark.django_db
def test_replace_premium_asset_file_preserves_existing_price_status_and_published_at(django_user_model):
    creator = django_user_model.objects.create_user(username="creator_replace")
    media = create_test_media(user=creator, friendly_token="replacepremium")
    asset = create_ready_asset(media=media, price_tokens=750 * 10**6)
    original_published_at = asset.premium_published_at

    uploaded_file = SimpleUploadedFile(
        "replacement.mp4",
        b"replacement-bytes",
        content_type="video/mp4",
    )

    with patch(
        "premium.services.upload_premium_file_to_private_s3",
        return_value={
            "storage_bucket": "premium-private",
            "storage_key": "premium-media/users/1/media/replacepremium/replacement.mp4",
            "content_type": "video/mp4",
            "file_name": "replacement.mp4",
            "size_bytes": len(b"replacement-bytes"),
        },
    ):
        updated = replace_creator_premium_asset_file(
            actor=creator,
            media=media,
            uploaded_file=uploaded_file,
        )

    updated.refresh_from_db()

    assert updated.id == asset.id
    assert updated.status == PremiumMediaAsset.STATUS_READY
    assert updated.price_tokens == 750 * 10**6
    assert updated.premium_published_at == original_published_at
    assert updated.storage_key == "premium-media/users/1/media/replacepremium/replacement.mp4"
    assert updated.file_name == "replacement.mp4"


@pytest.mark.django_db
def test_update_premium_settings_switches_draft_to_ready_and_sets_price(django_user_model):
    creator = django_user_model.objects.create_user(username="creator_settings")
    media = create_test_media(user=creator, friendly_token="settingspremium")
    asset = create_draft_asset(media=media)

    updated = update_creator_premium_asset_settings(
        actor=creator,
        media=media,
        price_display="12.5",
        publish_now=True,
    )

    assert updated.id == asset.id
    assert updated.status == PremiumMediaAsset.STATUS_READY
    assert updated.price_tokens == 12_500_000
    assert updated.premium_published_at is not None
    assert format_token_amount(updated.price_tokens) == "12.5"


@pytest.mark.django_db
def test_purchase_with_tokens_creates_purchase_unlock_ledger_entries_and_balances(django_user_model, settings):
    settings.PREMIUM_CREATOR_SHARE_BPS = 8000

    creator = django_user_model.objects.create_user(username="creator_purchase")
    buyer = django_user_model.objects.create_user(username="buyer_purchase")
    media = create_test_media(user=creator, friendly_token="purchasepremium")
    create_ready_asset(media=media, price_tokens=PRICE_TOKENS)

    buyer_wallet = fund_user_wallet(buyer, 1_000 * 10**6)
    creator_wallet = fund_user_wallet(creator, 0)

    result = purchase_premium_media_with_tokens(actor=buyer, media=media)

    buyer_wallet.refresh_from_db()
    creator_wallet.refresh_from_db()
    platform_wallet = TokenWallet.objects.get(
        wallet_type=TokenWallet.TYPE_SYSTEM,
        system_key=TokenWallet.SYSTEM_PLATFORM_FEES,
    )

    purchase = MediaPurchase.objects.get(user=buyer, media=media)
    unlock = PremiumMediaUnlock.objects.get(user=buyer, media=media)

    assert result["purchased"] is True
    assert result["already_unlocked"] is False
    assert result["unlock_id"] == unlock.id

    assert purchase.price_tokens == PRICE_TOKENS
    assert unlock.source_type == PremiumMediaUnlock.SOURCE_PURCHASE
    assert unlock.revoked_at is None

    assert buyer_wallet.balance == 500 * 10**6
    assert creator_wallet.balance == 400 * 10**6
    assert platform_wallet.balance == 100 * 10**6

    txn = purchase.txn
    assert txn.kind == "purchase"
    assert txn.metadata["product"] == "premium_media"
    assert txn.metadata["media_id"] == media.pk

    entries = list(LedgerEntry.objects.filter(txn=txn))
    assert len(entries) == 3
    assert sum(entry.delta for entry in entries) == 0


@pytest.mark.django_db
def test_purchase_with_insufficient_balance_does_not_create_unlock_or_ledger_rows(django_user_model):
    creator = django_user_model.objects.create_user(username="creator_insufficient")
    buyer = django_user_model.objects.create_user(username="buyer_insufficient")
    media = create_test_media(user=creator, friendly_token="insufficientpremium")
    create_ready_asset(media=media, price_tokens=PRICE_TOKENS)

    fund_user_wallet(buyer, 100 * 10**6)

    with pytest.raises(ValidationError, match="Insufficient token balance"):
        purchase_premium_media_with_tokens(actor=buyer, media=media)

    assert MediaPurchase.objects.filter(user=buyer, media=media).exists() is False
    assert PremiumMediaUnlock.objects.filter(user=buyer, media=media).exists() is False
    assert LedgerTransaction.objects.filter(kind="purchase").exists() is False


@pytest.mark.django_db
def test_purchase_by_creator_does_not_charge_tokens_but_creates_admin_unlock(django_user_model):
    creator = django_user_model.objects.create_user(username="creator_self_purchase")
    media = create_test_media(user=creator, friendly_token="selfpurchasepremium")
    create_ready_asset(media=media, price_tokens=PRICE_TOKENS)

    creator_wallet = fund_user_wallet(creator, 0)

    result = purchase_premium_media_with_tokens(actor=creator, media=media)

    creator_wallet.refresh_from_db()
    unlock = PremiumMediaUnlock.objects.get(user=creator, media=media)

    assert result["purchased"] is False
    assert result["already_unlocked"] is True
    assert result["price_tokens"] == 0
    assert unlock.source_type == PremiumMediaUnlock.SOURCE_ADMIN
    assert unlock.metadata["reason"] == "creator_owner"
    assert creator_wallet.balance == 0
    assert MediaPurchase.objects.filter(user=creator, media=media).exists() is False


@pytest.mark.django_db
def test_premium_playback_endpoint_rejects_locked_viewer_and_allows_unlocked_viewer(client, django_user_model):
    creator = django_user_model.objects.create_user(username="creator_endpoint", password="pass")
    buyer = django_user_model.objects.create_user(username="buyer_endpoint", password="pass")
    media = create_test_media(user=creator, friendly_token="endpointpremium")
    create_ready_asset(media=media)

    url = reverse("premium_media_playback", kwargs={"friendly_token": media.friendly_token})

    client.login(username="buyer_endpoint", password="pass")
    locked_response = client.get(url)
    assert locked_response.status_code == 403
    assert locked_response.json()["ok"] is False

    PremiumMediaUnlock.objects.create(
        user=buyer,
        media=media,
        source_type=PremiumMediaUnlock.SOURCE_PURCHASE,
    )

    unlocked_response = client.get(url)
    assert unlocked_response.status_code == 200
    payload = unlocked_response.json()

    assert payload["ok"] is True
    assert payload["playback_type"] == "premium"
    assert payload["encodings_info"]["1080"]["h264"]["url"] == "https://example.com/full.mp4"


@pytest.mark.django_db
def test_unlocked_media_api_includes_purchases_and_creator_owned_ready_assets(client, django_user_model):
    creator = django_user_model.objects.create_user(username="creator_library", password="pass")
    buyer = django_user_model.objects.create_user(username="buyer_library", password="pass")

    owned_media = create_test_media(
        user=creator,
        friendly_token="ownedlibrary",
        title="Owned premium",
    )
    purchased_media = create_test_media(
        user=buyer,
        friendly_token="purchasedlibrary",
        title="Purchased premium",
    )

    create_ready_asset(media=owned_media)
    create_ready_asset(media=purchased_media)

    PremiumMediaUnlock.objects.create(
        user=creator,
        media=purchased_media,
        source_type=PremiumMediaUnlock.SOURCE_PURCHASE,
    )

    client.login(username="creator_library", password="pass")
    response = client.get(reverse("premium_unlocked_media_api"))

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True

    results_by_token = {item["friendly_token"]: item for item in payload["results"]}

    assert set(results_by_token) == {"ownedlibrary", "purchasedlibrary"}
    assert results_by_token["ownedlibrary"]["source_type"] == "creator"
    assert results_by_token["purchasedlibrary"]["source_type"] == PremiumMediaUnlock.SOURCE_PURCHASE

    assert results_by_token["ownedlibrary"]["url"] == "/view?m=ownedlibrary&playback=premium"
    assert results_by_token["purchasedlibrary"]["url"] == "/view?m=purchasedlibrary&playback=premium"


@pytest.mark.django_db
def test_unlocked_media_page_renders_owned_ready_asset_without_explicit_unlock(client, django_user_model):
    creator = django_user_model.objects.create_user(username="creator_page", password="pass")
    media = create_test_media(
        user=creator,
        friendly_token="ownedpagepremium",
        title="Owned page premium",
    )
    create_ready_asset(media=media)

    client.login(username="creator_page", password="pass")
    response = client.get(reverse("premium_unlocked_media_page"))

    assert response.status_code == 200
    assert b"Owned page premium" in response.content
    assert b"/view?m=ownedpagepremium&amp;playback=premium" in response.content
    assert b"Storage:" not in response.content


@pytest.mark.django_db
def test_build_premium_watch_url_preserves_existing_media_query_parameter(django_user_model):
    creator = django_user_model.objects.create_user(username="creator_url")
    media = create_test_media(user=creator, friendly_token="urlpremium")

    assert media.get_absolute_url() == "/view?m=urlpremium"
    assert build_premium_watch_url(media) == "/view?m=urlpremium&playback=premium"

@pytest.mark.django_db
def test_generic_media_detail_uses_preview_by_default_for_unlocked_user(client, django_user_model):
    creator = django_user_model.objects.create_user(username="creator_preview_default", password="pass")
    buyer = django_user_model.objects.create_user(username="buyer_preview_default", password="pass")

    media = create_test_media(
        user=creator,
        friendly_token="previewdefault",
        title="Preview default premium",
    )

    media.encodings_info = {
        "720": {
            "h264": {
                "url": "https://cdn.example.com/preview.mp4",
                "status": "success",
                "progress": 100,
            }
        }
    }
    media.save(update_fields=["encodings_info"])

    create_ready_asset(
        media=media,
        direct_url="https://private.example.com/full.mp4",
    )

    PremiumMediaUnlock.objects.create(
        user=buyer,
        media=media,
        source_type=PremiumMediaUnlock.SOURCE_PURCHASE,
    )

    client.login(username="buyer_preview_default", password="pass")
    response = client.get(reverse("api_get_media", kwargs={"friendly_token": media.friendly_token}))

    assert response.status_code == 200

    payload = response.json()
    serialized = str(payload)

    assert "https://cdn.example.com/preview.mp4" in serialized
    assert "https://private.example.com/full.mp4" not in serialized


@pytest.mark.django_db
def test_generic_media_detail_uses_premium_only_when_explicitly_requested(client, django_user_model):
    creator = django_user_model.objects.create_user(username="creator_premium_explicit", password="pass")
    buyer = django_user_model.objects.create_user(username="buyer_premium_explicit", password="pass")

    media = create_test_media(
        user=creator,
        friendly_token="premiumexplicit",
        title="Explicit premium playback",
    )

    media.encodings_info = {
        "720": {
            "h264": {
                "url": "https://cdn.example.com/preview.mp4",
                "status": "success",
                "progress": 100,
            }
        }
    }
    media.save(update_fields=["encodings_info"])

    create_ready_asset(
        media=media,
        direct_url="https://private.example.com/full.mp4",
    )

    PremiumMediaUnlock.objects.create(
        user=buyer,
        media=media,
        source_type=PremiumMediaUnlock.SOURCE_PURCHASE,
    )

    client.login(username="buyer_premium_explicit", password="pass")
    response = client.get(
        reverse("api_get_media", kwargs={"friendly_token": media.friendly_token}),
        {"playback": "premium"},
    )

    assert response.status_code == 200

    payload = response.json()
    serialized = str(payload)

    assert "https://private.example.com/full.mp4" in serialized
    assert "https://cdn.example.com/preview.mp4" not in serialized