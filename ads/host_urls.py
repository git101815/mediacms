from django.urls import path

from . import views

urlpatterns = [
    path("login/", views.ads_login, name="ads_login"),
    path("auth/callback/", views.sso_callback, name="ads_sso_callback"),
    path("logout/", views.ads_logout, name="ads_logout"),
    path("", views.dashboard, name="ads_dashboard"),
    path("campaigns/new/", views.campaign_create, name="ads_campaign_create"),
    path("campaigns/<int:campaign_id>/edit/", views.campaign_edit, name="ads_campaign_edit"),
    path("campaigns/<int:campaign_id>/toggle/", views.campaign_toggle, name="ads_campaign_toggle"),
    path("creatives/", views.creatives, name="ads_creatives"),
    path("finance/", views.finance, name="wallet"),
    path("finance/deposit-request/", views.finance_deposit_request, name="wallet_deposit_request"),
    path("finance/deposits/<uuid:public_id>/", views.finance_deposit_session, name="wallet_deposit_session"),
    path("finance/deposits/<uuid:public_id>/status/", views.finance_deposit_session_status, name="wallet_deposit_session_status"),
    path("finance/deposits/<uuid:public_id>/cancel/", views.finance_deposit_session_cancel, name="wallet_deposit_session_cancel"),
    path("finance/deposits/<uuid:public_id>/dfx-launch/", views.finance_dfx_launch, name="wallet_dfx_launch"),
    path("finance/deposits/<uuid:public_id>/dfx-return/", views.finance_dfx_return, name="wallet_dfx_return"),
    path("finance/deposits/<uuid:public_id>/dfx-return/buy", views.finance_dfx_return, name="wallet_dfx_return_buy"),
    path("finance/deposits/<uuid:public_id>/mtpelerin-launch/", views.finance_mtpelerin_launch, name="wallet_mtpelerin_launch"),
    path("finance/deposits/<uuid:public_id>/banxa-launch/", views.finance_banxa_launch, name="wallet_banxa_launch"),
]
