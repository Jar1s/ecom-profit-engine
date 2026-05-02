"""Tests for Shopify payouts fee helpers."""

from __future__ import annotations

import unittest

import pandas as pd

from shopify_payouts import payment_net_by_order_id, payout_fees_monthly


class TestShopifyPayouts(unittest.TestCase):
    def test_payout_fees_monthly_groups_by_month(self) -> None:
        df = pd.DataFrame(
            [
                {"Date": "2026-01-01", "Fee_Amount": 1.25},
                {"Date": "2026-01-15", "Fee_Amount": 2.75},
                {"Date": "2026-02-01", "Fee_Amount": 3.0},
            ]
        )
        out = payout_fees_monthly(df)
        self.assertEqual(len(out), 2)
        jan = out[out["Month"] == "2026-01"].iloc[0]
        feb = out[out["Month"] == "2026-02"].iloc[0]
        self.assertEqual(jan["Payout_Fees_Total"], 4.0)
        self.assertEqual(feb["Payout_Fees_Total"], 3.0)

    def test_payment_net_by_order_id_sums_net_per_order(self) -> None:
        rows = [
            {"Source_Order_ID": "1001", "Net_Amount": 48.5},
            {"Source_Order_ID": "1001", "Net_Amount": -2.0},
            {"Source_Order_ID": "1002", "Net_Amount": 10.0},
            {"Source_Order_ID": "", "Net_Amount": 99.0},
        ]
        out = payment_net_by_order_id(rows)
        self.assertEqual(out[1001], 46.5)
        self.assertEqual(out[1002], 10.0)
        self.assertNotIn(0, out)


if __name__ == "__main__":
    unittest.main()
