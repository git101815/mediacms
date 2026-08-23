from decimal import Decimal

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from ads.providers import (
    FORMAT_IN_VIDEO,
    FORMAT_POPUNDER,
    eligible_provider_weights,
    provider_weights,
    weighted_provider_order,
)


BASE_SETTINGS = {
    "ADS_PROVIDER_WEIGHTS": {
        "internal": 50,
        "clickaine": 50,
        "partner": 0,
    },
    "CLICKAINE_POPUNDER_ENABLED": True,
    "CLICKAINE_POPUNDER_SCRIPT_URL": "https://clickaine.example/pop.js",
    "CLICKAINE_VAST_ENABLED": True,
    "CLICKAINE_VAST_URL": "https://clickaine.example/vast",
}


@override_settings(**BASE_SETTINGS)
def test_provider_weights_are_percentages_summing_to_100():
    assert provider_weights() == {
        "internal": Decimal("50"),
        "clickaine": Decimal("50"),
        "partner": Decimal("0"),
    }


@override_settings(
    **{
        **BASE_SETTINGS,
        "ADS_PROVIDER_WEIGHTS": {
            "internal": 50,
            "clickaine": 49,
            "partner": 0,
        },
    }
)
def test_provider_weights_reject_sum_other_than_100():
    with pytest.raises(ImproperlyConfigured, match="exactly 100"):
        provider_weights()


@override_settings(**BASE_SETTINGS)
def test_equal_weights_can_put_either_provider_first():
    first_internal = weighted_provider_order(
        FORMAT_POPUNDER,
        rng=lambda: 0.1,
    )
    first_clickaine = weighted_provider_order(
        FORMAT_POPUNDER,
        rng=lambda: 0.9,
    )
    assert first_internal == ["internal", "clickaine"]
    assert first_clickaine == ["clickaine", "internal"]


@override_settings(
    **{
        **BASE_SETTINGS,
        "CLICKAINE_POPUNDER_ENABLED": False,
    }
)
def test_disabled_clickaine_is_removed_and_remaining_weight_is_effective():
    assert eligible_provider_weights(FORMAT_POPUNDER) == {
        "internal": Decimal("50")
    }
    assert weighted_provider_order(FORMAT_POPUNDER) == ["internal"]


@override_settings(
    **{
        **BASE_SETTINGS,
        "ADS_PROVIDER_WEIGHTS": {
            "internal": 40,
            "clickaine": 40,
            "partner": 20,
        },
    }
)
def test_in_video_rejects_positive_partner_weight_without_vast_adapter():
    with pytest.raises(ImproperlyConfigured, match="no in-video/VAST adapter"):
        eligible_provider_weights(FORMAT_IN_VIDEO)
