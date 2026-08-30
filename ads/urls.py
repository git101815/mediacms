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
        "api/v1/ads/popunder/",
        views.ads_popunder,
        name="ads_popunder",
    ),
    path(
        "api/v1/ads/popunder/consume/",
        views.ads_popunder_consume,
        name="ads_popunder_consume",
    ),
    path(
        "api/v1/ads/vmap/",
        views.ads_vmap,
        name="ads_vmap",
    ),
    path(
        "api/v1/ads/vast/<slug:slot>/",
        views.ads_vast,
        name="ads_vast",
    ),
    path(
        "ads/clickaine-vast-impression/",
        views.clickaine_vast_impression,
        name="clickaine_vast_impression",
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
