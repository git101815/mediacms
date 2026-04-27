from __future__ import annotations


MONEY_FRAME_DENY_PATH_PREFIXES = (
    "/wallet",
)


def _merge_frame_ancestors_none(existing_csp: str) -> str:
    directives = []
    replaced = False

    for raw_directive in str(existing_csp or "").split(";"):
        directive = raw_directive.strip()
        if not directive:
            continue

        if directive.lower().startswith("frame-ancestors "):
            directives.append("frame-ancestors 'none'")
            replaced = True
        else:
            directives.append(directive)

    if not replaced:
        directives.append("frame-ancestors 'none'")

    return "; ".join(directives)


class MoneyFrameDenyMiddleware:
    """
    Deny framing on wallet and money-related surfaces while allowing the
    global site policy to remain compatible with explicit embed views.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)

        path = request.path or ""
        if any(path == prefix or path.startswith(f"{prefix}/") for prefix in MONEY_FRAME_DENY_PATH_PREFIXES):
            response["X-Frame-Options"] = "DENY"
            response["Content-Security-Policy"] = _merge_frame_ancestors_none(
                response.get("Content-Security-Policy", "")
            )

        return response