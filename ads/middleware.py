from urllib.parse import urlencode

from django.conf import settings
from django.shortcuts import redirect


class AdsHostMiddleware:
    """Route ADS_HOST to an independent advertiser UI and local AuthWall."""

    PUBLIC_PATHS = (
        "/login/",
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
            return redirect(f"/login/?{urlencode({'next': next_path})}")

        if not (
            getattr(request.user, "advertiserUser", False)
            or getattr(request.user, "is_superuser", False)
        ):
            return redirect("/login/?denied=1")

        return self.get_response(request)
