from django.conf import settings

from cms.version import VERSION

from .frontend_translations import get_translation, get_translation_strings
from .methods import is_mediacms_editor, is_mediacms_manager
import time

def stuff(request):
    """Pass settings to the frontend"""
    ret = {}
    ret["FRONTEND_HOST"] = request.build_absolute_uri('/').rstrip('/')
    ret["DEFAULT_THEME"] = settings.DEFAULT_THEME
    ret["PORTAL_NAME"] = settings.PORTAL_NAME
    ret["PORTAL_DESCRIPTION"] = settings.PORTAL_DESCRIPTION
    ret["LOAD_FROM_CDN"] = settings.LOAD_FROM_CDN
    ret["CAN_LOGIN"] = settings.LOGIN_ALLOWED
    ret["CAN_REGISTER"] = settings.REGISTER_ALLOWED
    ret["CAN_UPLOAD_MEDIA"] = settings.UPLOAD_MEDIA_ALLOWED
    ret["TIMESTAMP_IN_TIMEBAR"] = settings.TIMESTAMP_IN_TIMEBAR
    ret["CAN_MENTION_IN_COMMENTS"] = settings.ALLOW_MENTION_IN_COMMENTS
    ret["CAN_LIKE_MEDIA"] = settings.CAN_LIKE_MEDIA
    ret["CAN_DISLIKE_MEDIA"] = settings.CAN_DISLIKE_MEDIA
    ret["CAN_REPORT_MEDIA"] = settings.CAN_REPORT_MEDIA
    ret["CAN_SHARE_MEDIA"] = settings.CAN_SHARE_MEDIA
    ret["UPLOAD_MAX_SIZE"] = settings.UPLOAD_MAX_SIZE
    ret["UPLOAD_MAX_FILES_NUMBER"] = settings.UPLOAD_MAX_FILES_NUMBER
    ret["PRE_UPLOAD_MEDIA_MESSAGE"] = settings.PRE_UPLOAD_MEDIA_MESSAGE
    ret["POST_UPLOAD_AUTHOR_MESSAGE_UNLISTED_NO_COMMENTARY"] = settings.POST_UPLOAD_AUTHOR_MESSAGE_UNLISTED_NO_COMMENTARY
    ret["IS_MEDIACMS_ADMIN"] = request.user.is_superuser
    ret["IS_MEDIACMS_EDITOR"] = is_mediacms_editor(request.user)
    ret["IS_MEDIACMS_MANAGER"] = is_mediacms_manager(request.user)
    ret["ALLOW_RATINGS"] = settings.ALLOW_RATINGS
    ret["ALLOW_RATINGS_CONFIRMED_EMAIL_ONLY"] = settings.ALLOW_RATINGS_CONFIRMED_EMAIL_ONLY
    ret["VIDEO_PLAYER_FEATURED_VIDEO_ON_INDEX_PAGE"] = settings.VIDEO_PLAYER_FEATURED_VIDEO_ON_INDEX_PAGE
    ret["RSS_URL"] = "/rss"
    ret["TRANSLATION"] = get_translation(request.LANGUAGE_CODE)
    ret["REPLACEMENTS"] = get_translation_strings(request.LANGUAGE_CODE)
    ret["USE_SAML"] = settings.USE_SAML
    ret["USE_RBAC"] = settings.USE_RBAC
    ret["USE_ROUNDED_CORNERS"] = settings.USE_ROUNDED_CORNERS
    ret["VERSION"] = VERSION
    ret["IS_AD_FREE_USER"] = (
            request.user.is_authenticated
            and getattr(request.user, "adFreeUser", False)
    )

    if request.user.is_superuser:
        ret["DJANGO_ADMIN_URL"] = settings.DJANGO_ADMIN_URL

    ret["IS_ADVANCED_USER"] = (request.user.is_authenticated and getattr(request.user, "advancedUser", False))

    return ret

def ads_flags(request):
    # Advanced user, paid ad-free user, or Googlebot: no ads.
    is_gbot = getattr(request, "is_googlebot_verified", False)
    is_adv = (
        request.user.is_authenticated
        and getattr(request.user, "advancedUser", False)
    )
    is_ad_free = (
        request.user.is_authenticated
        and getattr(request.user, "adFreeUser", False)
    )

    if is_gbot:
        return {
            "IS_ADVANCED_USER": False,
            "IS_AD_FREE_USER": False,
            "SHOW_PREROLL": False,
            "SHOW_TABUNDER": False,
        }

    if is_adv or is_ad_free:
        return {
            "IS_ADVANCED_USER": is_adv,
            "IS_AD_FREE_USER": is_ad_free,
            "SHOW_PREROLL": False,
            "SHOW_TABUNDER": False,
        }

    now = int(time.time())
    media_page = bool(getattr(request, "media_page", False))
    preroll_eligible = bool(
        getattr(request, "preroll_eligible", False)
    )

    show_tabunder = False
    if media_page:
        cooldown_tab = int(settings.TABUNDER_COOLDOWN_SECONDS)
        last_tab = request.session.get("tabunder_last_ts")
        if not isinstance(last_tab, int) or last_tab > now:
            last_tab = None
        show_tabunder = (
            last_tab is None
            or now - last_tab >= cooldown_tab
        )

    show_preroll = False
    if preroll_eligible:
        cooldown_pre = int(settings.PREROLLS_COOLDOWN_SECONDS)
        last_pre = request.session.get("preroll_last_ts")
        if not isinstance(last_pre, int) or last_pre > now:
            last_pre = None
        show_preroll = (
            last_pre is None
            or now - last_pre >= cooldown_pre
        )

    if show_tabunder:
        request.session["tabunder_last_ts"] = now
    if show_preroll:
        request.session["preroll_last_ts"] = now

    return {
        "IS_ADVANCED_USER": False,
        "IS_AD_FREE_USER": False,
        "SHOW_PREROLL": show_preroll,
        "SHOW_TABUNDER": show_tabunder,
    }
