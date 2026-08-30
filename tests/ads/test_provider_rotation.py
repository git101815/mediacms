
from decimal import Decimal

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from ads.providers import (
    FORMAT_IN_VIDEO,
    FORMAT_POPUNDER,
    eligible_provider_weights,
    has_eligible_provider,
    partner_popunder_url,
    provider_weights,
    weighted_provider_order,
)


BASE_SETTINGS = {
    "POPUNDER_ADS_ENABLED": True,
    "IN_VIDEO_ADS_ENABLED": True,
    "ADS_PROVIDER_WEIGHTS": {
        "popunder": {
            "internal": 50,
            "clickaine": 50,
            "partner": 0,
        },
        "in_video": {
            "internal": 50,
            "clickaine": 50,
            "partner": 0,
        },
    },
    "CLICKAINE_POPUNDER_ENABLED": True,
    "CLICKAINE_POPUNDER_SCRIPT_URL": "https://clickaine.example/pop.js",
    "CLICKAINE_VAST_ENABLED": True,
    "CLICKAINE_VAST_URL": "https://clickaine.example/vast",
    "ADS_PARTNER_POPUNDER_OFFERS": [
        {
            "weight": 100,
            "url_template": "https://partner.example/open?click=CLICKID",
        }
    ],
}


@override_settings(**BASE_SETTINGS)
def test_provider_weights_are_scoped_per_format():
    assert provider_weights(FORMAT_POPUNDER) == {
        "internal": Decimal("50"),
        "clickaine": Decimal("50"),
        "partner": Decimal("0"),
    }
    assert provider_weights(FORMAT_IN_VIDEO) == {
        "internal": Decimal("50"),
        "clickaine": Decimal("50"),
        "partner": Decimal("0"),
    }


@override_settings(
    **{
        **BASE_SETTINGS,
        "ADS_PROVIDER_WEIGHTS": {
            **BASE_SETTINGS["ADS_PROVIDER_WEIGHTS"],
            "popunder": {
                "internal": 50,
                "clickaine": 49,
                "partner": 0,
            },
        },
    }
)
def test_provider_weights_reject_sum_other_than_100_per_format():
    with pytest.raises(ImproperlyConfigured, match="exactly 100"):
        provider_weights(FORMAT_POPUNDER)


@override_settings(**BASE_SETTINGS)
def test_equal_weights_can_put_either_provider_first():
    first_internal = weighted_provider_order(FORMAT_POPUNDER, rng=lambda: 0.1)
    first_clickaine = weighted_provider_order(FORMAT_POPUNDER, rng=lambda: 0.9)
    assert first_internal == ["internal", "clickaine"]
    assert first_clickaine == ["clickaine", "internal"]


@override_settings(**{**BASE_SETTINGS, "POPUNDER_ADS_ENABLED": False})
def test_format_kill_switch_removes_entire_popunder_pipeline():
    assert eligible_provider_weights(FORMAT_POPUNDER) == {}
    assert weighted_provider_order(FORMAT_POPUNDER) == []
    assert has_eligible_provider(FORMAT_POPUNDER) is False


@override_settings(
    **{
        **BASE_SETTINGS,
        "CLICKAINE_POPUNDER_ENABLED": False,
    }
)
def test_disabled_clickaine_is_removed_without_affecting_internal():
    assert eligible_provider_weights(FORMAT_POPUNDER) == {
        "internal": Decimal("50")
    }
    assert weighted_provider_order(FORMAT_POPUNDER) == ["internal"]


@override_settings(
    **{
        **BASE_SETTINGS,
        "ADS_PROVIDER_WEIGHTS": {
            **BASE_SETTINGS["ADS_PROVIDER_WEIGHTS"],
            "popunder": {
                "internal": 40,
                "clickaine": 40,
                "partner": 20,
            },
        },
    }
)
def test_partner_weight_is_valid_for_popunder_only():
    eligible = eligible_provider_weights(FORMAT_POPUNDER)
    assert eligible["partner"] == Decimal("20")
    assert "partner" not in eligible_provider_weights(FORMAT_IN_VIDEO)


@override_settings(
    **{
        **BASE_SETTINGS,
        "ADS_PROVIDER_WEIGHTS": {
            **BASE_SETTINGS["ADS_PROVIDER_WEIGHTS"],
            "in_video": {
                "internal": 40,
                "clickaine": 40,
                "partner": 20,
            },
        },
    }
)
def test_in_video_rejects_positive_partner_weight_without_vast_adapter():
    with pytest.raises(ImproperlyConfigured, match="no in-video/VAST adapter"):
        eligible_provider_weights(FORMAT_IN_VIDEO)


@override_settings(**BASE_SETTINGS)
def test_partner_offer_is_resolved_server_side():
    url = partner_popunder_url(rng=lambda: 0.1, click_id="abc 123")
    assert url == "https://partner.example/open?click=abc%20123&focus=0"
