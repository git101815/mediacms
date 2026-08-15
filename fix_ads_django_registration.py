#!/usr/bin/env python3
"""
Repair the Django/Celery registration hooks for the self-serve Ads app.

Run from the MediaCMS repository root:
    python3 fix_ads_django_registration.py

The script is idempotent: already-correct hooks are left unchanged.
"""

from pathlib import Path
import sys

ROOT = Path.cwd()


def die(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, content: str) -> None:
    (ROOT / path).write_text(content, encoding="utf-8")


def require(path: str) -> None:
    if not (ROOT / path).exists():
        die(f"Missing required file: {path}")


def replace_once(path: str, old: str, new: str, marker: str) -> None:
    text = read(path)
    if marker in text:
        print(f"ok    {path}: {marker}")
        return
    count = text.count(old)
    if count != 1:
        die(
            f"Cannot safely patch {path}: expected exactly one anchor for {marker}, "
            f"found {count}"
        )
    write(path, text.replace(old, new, 1))
    print(f"fix   {path}: {marker}")


def main() -> None:
    for path in (
        "manage.py",
        "cms/settings.py",
        "cms/urls.py",
        "deploy/docker/local_settings.py",
        "ads/apps.py",
        "ads/models.py",
        "ads/urls.py",
        "ads/tasks.py",
        "ads/middleware.py",
    ):
        require(path)

    # 1. This is the fatal missing hook from the supplied traceback.
    replace_once(
        "cms/settings.py",
        '    "imagekit",\n'
        '    "ledger.apps.LedgerConfig",\n'
        '    "premium.apps.PremiumConfig",\n',
        '    "imagekit",\n'
        '    "ledger.apps.LedgerConfig",\n'
        '    "ads.apps.AdsConfig",\n'
        '    "premium.apps.PremiumConfig",\n',
        '"ads.apps.AdsConfig"',
    )

    # 2. AuthWall / host routing middleware must be active.
    replace_once(
        "cms/settings.py",
        '    "django.contrib.auth.middleware.AuthenticationMiddleware",\n'
        '    "django.contrib.messages.middleware.MessageMiddleware",\n',
        '    "django.contrib.auth.middleware.AuthenticationMiddleware",\n'
        '    "ads.middleware.AdsHostMiddleware",\n'
        '    "django.contrib.messages.middleware.MessageMiddleware",\n',
        '"ads.middleware.AdsHostMiddleware"',
    )

    # 3. Default Ads runtime settings.
    replace_once(
        "cms/settings.py",
        'DJANGO_ADMIN_URL = "admin/"\n\n'
        '# this are used around a number of places and will need to be well documented!!!\n',
        'DJANGO_ADMIN_URL = "admin/"\n\n'
        '# Self-serve direct advertising. Production overrides ADS_HOST in local_settings.py.\n'
        'ADS_HOST = os.environ.get("ADS_HOST", "ads.localhost").strip().lower()\n'
        'ADS_SCHEME = os.environ.get("ADS_SCHEME", "https").strip().lower()\n'
        'ADS_SSO_TICKET_MAX_AGE_SECONDS = 60\n'
        'ADS_CLICK_TOKEN_MAX_AGE_SECONDS = 7 * 24 * 60 * 60\n'
        '# CPC campaigns are ranked against CPM using a smoothed 1% CTR prior.\n'
        'ADS_CPC_PRIOR_IMPRESSIONS = 1000\n'
        'ADS_CPC_PRIOR_CTR_PPM = 10000\n\n'
        '# this are used around a number of places and will need to be well documented!!!\n',
        'ADS_CPC_PRIOR_IMPRESSIONS = 1000',
    )

    # 4. Redis runtime refresh + Redis -> ledger settlement.
    replace_once(
        "cms/settings.py",
        '    "update_listings_thumbnails": {\n'
        '        "task": "update_listings_thumbnails",\n'
        '        "schedule": crontab(minute=2, hour="*/30"),\n'
        '    },\n'
        '}\n'
        '# TODO: beat, delete chunks from media root\n',
        '    "update_listings_thumbnails": {\n'
        '        "task": "update_listings_thumbnails",\n'
        '        "schedule": crontab(minute=2, hour="*/30"),\n'
        '    },\n'
        '    "ads_refresh_runtime": {\n'
        '        "task": "ads.refresh_runtime_state",\n'
        '        "schedule": 10.0,\n'
        '    },\n'
        '    "ads_settle_runtime": {\n'
        '        "task": "ads.settle_runtime",\n'
        '        "schedule": 15.0,\n'
        '    },\n'
        '}\n'
        '# TODO: beat, delete chunks from media root\n',
        '"ads_settle_runtime"',
    )

    # 5. URL registration.
    replace_once(
        "cms/urls.py",
        '    re_path(r"^", include("files.urls")),\n',
        '    re_path(r"^", include("ads.urls")),\n'
        '    re_path(r"^", include("files.urls")),\n',
        'include("ads.urls")',
    )

    # 6. Production subdomain.
    replace_once(
        "deploy/docker/local_settings.py",
        'FRONTEND_HOST = "https://celebfakes.ru"\n'
        'TIME_ZONE = "Europe/Moscow"\n',
        'FRONTEND_HOST = "https://celebfakes.ru"\n'
        'ADS_HOST = os.getenv("ADS_HOST", "ads.celebfakes.ru").strip().lower()\n'
        'ADS_SCHEME = "https"\n'
        'TIME_ZONE = "Europe/Moscow"\n',
        'ADS_HOST = os.getenv("ADS_HOST", "ads.celebfakes.ru")',
    )

    replace_once(
        "deploy/docker/local_settings.py",
        '    "www.celebfakes.ru",\n'
        '    "celebfakes.ru",\n'
        '    "medias.celebfakes.ru",\n',
        '    "www.celebfakes.ru",\n'
        '    "celebfakes.ru",\n'
        '    "ads.celebfakes.ru",\n'
        '    "medias.celebfakes.ru",\n',
        '    "ads.celebfakes.ru",',
    )

    print()
    print("Ads Django registration repaired.")
    print("Next:")
    print("  python3 manage.py check")
    print("  python3 manage.py migrate")
    print("Then recreate/restart web, celery_worker and celery_beat.")


if __name__ == "__main__":
    main()
