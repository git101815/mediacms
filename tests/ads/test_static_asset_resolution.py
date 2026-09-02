from pathlib import Path

from django.conf import settings
from django.contrib.staticfiles import finders


def test_ads_static_assets_resolve_to_canonical_source():
    expected = Path(settings.BASE_DIR) / 'static' / 'ads'

    for filename in ('ads.css', 'ads.js'):
        found = finders.find(f'ads/{filename}')
        assert found is not None
        assert Path(found).resolve() == (expected / filename).resolve()

def test_admin_vendor_assets_resolve_from_installed_static_provider():
    # Historical static/vendor copies were collectstatic output. The installed
    # provider (Jazzmin/admin stack) remains the source for these public paths.
    for asset in (
        "vendor/adminlte/css/adminlte.min.css",
        "vendor/adminlte/js/adminlte.min.js",
        "vendor/fontawesome-free/css/all.min.css",
        "vendor/select2/css/select2.min.css",
    ):
        assert finders.find(asset) is not None
