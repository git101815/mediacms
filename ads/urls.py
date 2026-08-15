from django.urls import path

from . import views

urlpatterns = [
    path("ads/sso/start/", views.sso_start, name="ads_sso_start"),
    path(
        "api/v1/direct-ads/serve/<slug:slot>/",
        views.serve_direct_ad,
        name="direct_ad_serve",
    ),
    path("ads/click/<str:token>/", views.ad_click, name="direct_ad_click"),
]
