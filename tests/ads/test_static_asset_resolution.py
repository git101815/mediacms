from pathlib import Path

from django.conf import settings
from django.contrib.staticfiles import finders


def test_ads_static_assets_resolve_to_canonical_source():
    expected = Path(settings.BASE_DIR) / 'static' / 'ads'

    for filename in ('ads.css', 'ads.js'):
        found = finders.find(f'ads/{filename}')
        assert found is not None
        assert Path(found).resolve() == (expected / filename).resolve()
