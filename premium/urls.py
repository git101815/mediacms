from django.urls import re_path

from . import views


urlpatterns = [
    re_path(
        (
            r"^premium/media/"
            r"(?P<friendly_token>[\w\-_]*)/"
            r"upload/$"
        ),
        views.PremiumFineUploaderView.as_view(),
        name="premium_media_asset_upload",
    ),
    re_path(
        (
            r"^premium/media/"
            r"(?P<friendly_token>[\w\-_]*)/"
            r"upload/"
            r"(?P<upload_uuid>[a-fA-F0-9\-]{36})/"
            r"status/$"
        ),
        views.premium_upload_status,
        name="premium_media_asset_upload_status",
    ),
    re_path(
        (
            r"^premium/media/"
            r"(?P<friendly_token>[\w\-_]*)/"
            r"edit$"
        ),
        views.creator_premium_asset_edit,
        name="premium_media_asset_edit",
    ),
    re_path(
        (
            r"^api/v1/media/"
            r"(?P<friendly_token>[\w\-_]*)/"
            r"purchase$"
        ),
        views.purchase_media,
        name="premium_media_purchase",
    ),
    re_path(
        (
            r"^api/v1/media/"
            r"(?P<friendly_token>[\w\-_]*)/"
            r"premium-playback$"
        ),
        views.premium_playback,
        name="premium_media_playback",
    ),
    re_path(
        r"^api/v1/me/unlocked-media$",
        views.unlocked_media_api,
        name="premium_unlocked_media_api",
    ),
    re_path(
        r"^unlocked$",
        views.unlocked_media_page,
        name="premium_unlocked_media_page",
    ),
]