# cms/middleware_googleflag.py

import socket

from functools import lru_cache


def _client_ip(request):

    ip = request.META.get("HTTP_CF_CONNECTING_IP")

    if ip:

        return ip

    xff = request.META.get("HTTP_X_FORWARDED_FOR")

    if xff:

        return xff.split(",")[0].strip()

    return request.META.get("REMOTE_ADDR")


@lru_cache(maxsize=2048)

def _rdns(ip):

    try:

        host, _, _ = socket.gethostbyaddr(ip)

        return host

    except Exception:

        return None


@lru_cache(maxsize=2048)

def _fdns(host):

    try:

        return socket.gethostbyname(host)

    except Exception:

        return None


class GooglebotFlagMiddleware:

    def __init__(self, get_response):

        self.get_response = get_response


    def __call__(self, request):

        ua   = request.META.get("HTTP_USER_AGENT", "")

        ip   = _client_ip(request)

        host = None

        verified = False


        if (("Googlebot" in ua) or ("Google-InspectionTool" in ua)) and ip:

            host = _rdns(ip)

            if host and (host.endswith(".googlebot.com") or host.endswith(".google.com")):

                ip2 = _fdns(host)

                verified = (ip2 == ip)


        request.is_googlebot_verified = bool(verified)

        request.googlebot_meta = {"ip": ip, "host": host}

        return self.get_response(request)

