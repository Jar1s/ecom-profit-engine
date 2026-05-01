"""Sanity checks for Shopify HTTP error hints."""

import unittest

from http_retry import _shopify_403_hint, _shopify_payments_403_followup


class TestShopify403Hints(unittest.TestCase):
    def test_payouts_followup_includes_checklist(self) -> None:
        s = _shopify_payments_403_followup("merchant approval for read_shopify_payments_payouts")
        self.assertIn("[1]", s)
        self.assertIn("Shopify Payments", s)
        self.assertIn("[5]", s)

    def test_shopify_403_hint_payouts_url(self) -> None:
        hint = _shopify_403_hint(
            "https://x.myshopify.com/admin/api/2024-10/shopify_payments/payouts.json",
            api_detail="merchant approval",
        )
        self.assertIn("Payouts 403", hint)

    def test_shopify_403_hint_orders_url(self) -> None:
        hint = _shopify_403_hint(
            "https://x.myshopify.com/admin/api/2024-10/orders.json",
            api_detail="",
        )
        self.assertIn("read_orders", hint)


if __name__ == "__main__":
    unittest.main()
