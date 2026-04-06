"""Meta funnel conversion counts from actions."""

from meta_ads import _ADD_TO_CART_ACTION_TYPES, conversion_count_from_actions


def test_add_to_cart_prefers_pixel_type() -> None:
    actions = [
        {"action_type": "offsite_conversion.fb_pixel_add_to_cart", "value": "12"},
        {"action_type": "add_to_cart", "value": "12"},
    ]
    assert conversion_count_from_actions(actions, _ADD_TO_CART_ACTION_TYPES) == 12.0


def test_add_to_cart_fallback() -> None:
    actions = [{"action_type": "add_to_cart", "value": "3"}]
    assert conversion_count_from_actions(actions, _ADD_TO_CART_ACTION_TYPES) == 3.0
