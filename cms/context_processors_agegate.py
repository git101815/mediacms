def age_gate_context(request):

    is_gbot = getattr(request, "is_googlebot_verified", False)

    meta    = getattr(request, "googlebot_meta", {}) or {}

    bot_comment = ""

    if is_gbot:

        ip   = meta.get("ip") or ""

        host = meta.get("host") or ""

        bot_comment = f"googlebot-verified ip={ip} host={host}"


    return {

        "SHOW_AGE_GATE": False,

        "GOOGLEBOT_DEBUG_COMMENT": bot_comment,

    }
