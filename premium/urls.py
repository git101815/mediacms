from django.urls import re_path

from . import views


urlpatterns = [
    re_path(
        r"^premium/media/(?P<friendly_token>[\w\-_]*)/edit$",
        views.creator_premium_asset_edit,
        name="premium_media_asset_edit",
    ),
    re_path(
        r"^api/v1/media/(?P<friendly_token>[\w\-_]*)/purchase$",
        views.purchase_media,
        name="premium_media_purchase",
    ),
    re_path(
        r"^api/v1/media/(?P<friendly_token>[\w\-_]*)/premium-playback$",
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