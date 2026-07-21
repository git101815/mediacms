#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

SERVICES_PATH = Path("ledger/services.py")
TEMPLATE_PATH = Path("templates/cms/mtpelerin_launch.html")

OLD_KIND = 'kind="mtpelerin_deposit_pending_reversal"'
NEW_KIND = 'kind="mtpelerin_pending_reversal"'

NEW_TEMPLATE = r'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="robots" content="noindex,nofollow,noarchive">
  <title>Mt Pelerin checkout</title>
  <style nonce="{{ mtpelerin_csp_nonce }}">
    :root { color-scheme: dark; }
    html, body {
      width: 100%;
      height: 100%;
      margin: 0;
      overflow: hidden;
      background: #101114;
      color: #f3f4f6;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    body {
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
    }
    .mtp-toolbar {
      min-height: 44px;
      box-sizing: border-box;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 8px 14px;
      border-bottom: 1px solid #2a2d33;
      background: #17191e;
      font-size: 14px;
    }
    .mtp-toolbar__status {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .mtp-toolbar__links {
      display: flex;
      flex: 0 0 auto;
      gap: 12px;
    }
    .mtp-toolbar a { color: #f3f4f6; }
    .mtp-frame {
      display: block;
      width: 100%;
      height: 100%;
      border: 0;
      background: #101114;
    }
  </style>
</head>
<body>
  <header class="mtp-toolbar">
    <span class="mtp-toolbar__status" id="mtp-status">
      Opening secure checkout…
    </span>
    <nav class="mtp-toolbar__links" aria-label="Mt Pelerin checkout navigation">
      <a href="{{ mtpelerin_launch.session_url }}">Top-up status</a>
      <a href="{{ mtpelerin_launch.wallet_url }}">Wallet</a>
    </nav>
  </header>

  <iframe
    id="mtp-widget-frame"
    class="mtp-frame"
    allow="usb; ethereum; clipboard-write; payment; microphone; camera"
    referrerpolicy="origin"
    title="Mt Pelerin exchange widget"
  ></iframe>

  <form hidden>
    {% csrf_token %}
  </form>
  <script nonce="{{ mtpelerin_csp_nonce }}">
  (function () {
    "use strict";

    const widgetOrigin = "{{ mtpelerin_widget_origin|escapejs }}";
    const eventUrl = "{{ mtpelerin_launch.event_url|escapejs }}";
    const sessionUrl = "{{ mtpelerin_launch.session_url|escapejs }}";
    const statusNode = document.getElementById("mtp-status");
    const frameNode = document.getElementById("mtp-widget-frame");
    const csrfNode = document.querySelector("[name=csrfmiddlewaretoken]");
    const widgetOptionsJson = (
      "{{ mtpelerin_widget_options_json|escapejs }}"
    );
    const sentKeys = new Set();

    function parseMessage(value) {
      if (typeof value === "string") {
        try {
          return JSON.parse(value);
        } catch (error) {
          return null;
        }
      }
      return value;
    }

    async function sendEvent(message) {
      const eventType = String(message.type || "");
      if (eventType !== "orderCreated" && eventType !== "paymentSubmitted") {
        return;
      }

      const data = (
        message.data && typeof message.data === "object"
      ) ? message.data : {};
      const eventKey = eventType + ":" + String(
        data.paymentId || data.id || ""
      );
      if (sentKeys.has(eventKey)) {
        return;
      }
      sentKeys.add(eventKey);

      const response = await fetch(eventUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": csrfNode ? csrfNode.value : "",
          "X-Requested-With": "XMLHttpRequest"
        },
        body: JSON.stringify({
          type: eventType,
          data: data
        })
      });

      const payload = await response.json().catch(function () {
        return {};
      });
      if (!response.ok) {
        sentKeys.delete(eventKey);
        throw new Error(payload.error || "Unable to record Mt Pelerin event");
      }

      if (eventType === "orderCreated") {
        statusNode.textContent = "Bank transfer instructions generated.";
      } else {
        statusNode.textContent = (
          "Waiting for transaction to complete (can take several days)."
        );
        window.setTimeout(function () {
          window.location.assign(sessionUrl);
        }, 1200);
      }
    }

    window.addEventListener("message", function (event) {
      if (
        event.origin !== widgetOrigin
        || event.source !== frameNode.contentWindow
      ) {
        return;
      }
      const message = parseMessage(event.data);
      if (!message || typeof message !== "object") {
        return;
      }
      sendEvent(message).catch(function (error) {
        statusNode.textContent = error.message;
      });
    });

    frameNode.addEventListener("load", function () {
      if (statusNode.textContent.indexOf("Opening") === 0) {
        statusNode.textContent = "Secure checkout loaded.";
      }
    });

    try {
      const options = JSON.parse(widgetOptionsJson);
      options.type = "web";

      const widgetUrl = new URL(widgetOrigin + "/");
      Object.keys(options).forEach(function (key) {
        const value = options[key];
        if (value !== null && value !== undefined && value !== "") {
          widgetUrl.searchParams.set(key, String(value));
        }
      });
      frameNode.src = widgetUrl.toString();
    } catch (error) {
      statusNode.textContent = error.message;
    }
  })();
  </script>
</body>
</html>
'''


def fail(message: str) -> None:
    raise RuntimeError(message)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fix only the Mt Pelerin pending-credit expiry kind and the broken "
            "showMtpModal launch."
        )
    )
    parser.add_argument("--root", default=".", help="MediaCMS repository root")
    parser.add_argument("--check", action="store_true", help="Validate without writing")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    services_path = root / SERVICES_PATH
    template_path = root / TEMPLATE_PATH

    if not services_path.is_file():
        fail(f"Missing file: {SERVICES_PATH}")
    if not template_path.is_file():
        fail(f"Missing file: {TEMPLATE_PATH}")

    services = services_path.read_text(encoding="utf-8")
    template = template_path.read_text(encoding="utf-8")

    changes: list[tuple[Path, str]] = []

    old_kind_count = services.count(OLD_KIND)
    new_kind_count = services.count(NEW_KIND)
    if old_kind_count == 1:
        services = services.replace(OLD_KIND, NEW_KIND, 1)
        changes.append((services_path, services))
    elif old_kind_count == 0 and new_kind_count >= 1:
        pass
    else:
        fail(
            f"{SERVICES_PATH}: expected one old Mt Pelerin reversal kind, "
            f"found {old_kind_count}"
        )

    modal_present = "window.showMtpModal(options);" in template
    iframe_present = (
        'id="mtp-widget-frame"' in template
        and "window.showMtpModal" not in template
    )
    if modal_present:
        changes.append((template_path, NEW_TEMPLATE))
    elif iframe_present:
        pass
    else:
        fail(
            f"{TEMPLATE_PATH}: neither the expected showMtpModal template nor "
            "the fixed iframe template was found"
        )

    if len("mtpelerin_pending_reversal") > 32:
        fail("Internal error: replacement transaction kind exceeds 32 characters")

    if args.check:
        if changes:
            print("Patch check passed. Files that would change:")
            for path, _content in changes:
                print(f"  {path.relative_to(root)}")
        else:
            print("Patch already applied.")
        return 0

    for path, content in changes:
        path.write_text(content, encoding="utf-8")
        print(f"updated {path.relative_to(root)}")

    if changes:
        print("Mt Pelerin expiry and launch fixes applied.")
    else:
        print("Patch already applied.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
