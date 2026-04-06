"""Purchase metrics from Meta insights: single action_type, no duplicate summing."""

from meta_ads import purchase_metrics_from_insights

_DEFAULT_ORDER = (
    "offsite_conversion.fb_pixel_purchase",
    "purchase",
    "omni_purchase",
    "onsite_conversion.purchase",
)


def test_picks_first_priority_type_not_sum_of_all_purchase_like() -> None:
    """Two overlapping types would previously double-count if summed."""
    actions = [
        {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "21"},
        {"action_type": "purchase", "value": "21"},
    ]
    action_values = [
        {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "1200.50"},
        {"action_type": "purchase", "value": "1200.50"},
    ]
    c, v = purchase_metrics_from_insights(
        actions, action_values, purchase_action_types=_DEFAULT_ORDER
    )
    assert c == 21.0
    assert v == 1200.50


def test_falls_back_to_purchase_when_pixel_missing() -> None:
    actions = [{"action_type": "purchase", "value": "5"}]
    action_values = [{"action_type": "purchase", "value": "99"}]
    c, v = purchase_metrics_from_insights(
        actions, action_values, purchase_action_types=_DEFAULT_ORDER
    )
    assert c == 5.0
    assert v == 99.0


def test_same_key_for_count_and_value() -> None:
    """If only value exists on pixel type and count on purchase, pick first in order that has either."""
    actions = [{"action_type": "purchase", "value": "3"}]
    action_values = [{"action_type": "offsite_conversion.fb_pixel_purchase", "value": "50"}]
    c, v = purchase_metrics_from_insights(
        actions, action_values, purchase_action_types=_DEFAULT_ORDER
    )
    # pixel is first in order and appears in action_values → key is pixel; count defaults 0
    assert c == 0.0
    assert v == 50.0
