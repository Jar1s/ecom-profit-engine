"""Tests for Shopify payouts fee helpers."""

from __future__ import annotations

import unittest

import pandas as pd

from shopify_payouts import payout_fees_monthly


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


if __name__ == "__main__":
    unittest.main()
