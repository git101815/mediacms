from django.urls import path

from . import views

urlpatterns = [
    path("", views.dashboard, name="ads_dashboard"),
    path("auth/callback/", views.sso_callback, name="ads_sso_callback"),
    path("logout/", views.ads_logout, name="ads_logout"),
    path("campaigns/new/", views.campaign_create, name="ads_campaign_create"),
    path(
        "campaigns/<int:campaign_id>/edit/",
        views.campaign_edit,
        name="ads_campaign_edit",
    ),
    path(
        "campaigns/<int:campaign_id>/toggle/",
        views.campaign_toggle,
        name="ads_campaign_toggle",
    ),
]
