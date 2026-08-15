from django.urls import path

from . import views

urlpatterns = [
    path("ads/sso/start/", views.sso_start, name="ads_sso_start"),
    path(
        "api/v1/direct-ads/serve/<slug:slot>/",
        views.serve_direct_ad,
        name="direct_ad_serve",
    ),
    path(
        "api/v1/direct-ads/reserve/<slug:slot>/",
        views.reserve_direct_ad,
        name="reserve_direct_ad",
    ),
    path(
        "api/v1/direct-ads/vmap/",
        views.direct_ads_vmap,
        name="direct_ads_vmap",
    ),
    path(
        "api/v1/direct-ads/vast/<slug:slot>/",
        views.direct_ads_vast,
        name="direct_ads_vast",
    ),
    path(
        "ads/impression/<str:token>/",
        views.direct_ad_impression,
        name="direct_ad_impression",
    ),
    path(
        "ads/track-click/<str:token>/",
        views.direct_ad_track_click,
        name="direct_ad_track_click",
    ),
    path(
        "ads/open/<str:token>/",
        views.direct_ad_open,
        name="direct_ad_open",
    ),
    path("ads/click/<str:token>/", views.ad_click, name="direct_ad_click"),
]
