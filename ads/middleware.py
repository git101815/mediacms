from urllib.parse import urlencode

from django.conf import settings
from django.http import HttpResponseForbidden
from django.shortcuts import redirect


class AdsHostMiddleware:
    """
    Route ADS_HOST to its own URLconf and make the entire host an advertiser
    AuthWall. The main site remains on cms.urls.
    """

    PUBLIC_PATHS = (
        "/auth/callback/",
    )

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        host = request.get_host().split(":", 1)[0].lower()
        ads_host = str(getattr(settings, "ADS_HOST", "") or "").strip().lower()
        if not ads_host or host != ads_host:
            return self.get_response(request)

        request.urlconf = "ads.host_urls"

        if request.path.startswith(self.PUBLIC_PATHS):
            return self.get_response(request)

        if not getattr(request.user, "is_authenticated", False):
            next_path = request.get_full_path()
            if not next_path.startswith("/") or next_path.startswith("//"):
                next_path = "/"
            main_host = str(settings.FRONTEND_HOST).rstrip("/")
            query = urlencode({"next": next_path})
            return redirect(f"{main_host}/ads/sso/start/?{query}")

        if not (
            getattr(request.user, "advertiserUser", False)
            or getattr(request.user, "is_superuser", False)
        ):
            return HttpResponseForbidden(
                "This account does not have advertiser access."
            )

        return self.get_response(request)
