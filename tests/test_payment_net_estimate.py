"""Tests for optional Payment_Net_Estimate when payout ledger net is missing."""

from __future__ import annotations

import unittest
from types import SimpleNamespace

import numpy as np
import pandas as pd

from payment_net_estimate import (
    apply_payment_net_estimate,
    classify_payment_bucket,
    estimate_net_from_revenue,
)
from transform import order_level_summary


class TestPaymentNetEstimate(unittest.TestCase):
    def test_classify_payment_bucket(self) -> None:
        self.assertEqual(classify_payment_bucket("paypal"), "paypal")
        self.assertEqual(classify_payment_bucket("Shopify Payments"), "shopify_payments")
        self.assertEqual(classify_payment_bucket("bogus"), "other")
        self.assertEqual(classify_payment_bucket("credit_card"), "shopify_payments")
        self.assertEqual(classify_payment_bucket("shop_pay"), "shopify_payments")
        self.assertEqual(classify_payment_bucket("shopify"), "shopify_payments")
        self.assertEqual(classify_payment_bucket("Apple Pay (via Shopify Payments)"), "shopify_payments")
        self.assertEqual(
            classify_payment_bucket("Shopify Payments — Visa · ····4242"),
            "shopify_payments",
        )

    def test_estimate_net_from_revenue(self) -> None:
        fees = {
            "paypal": {"pct": 0.1, "fixed": 1.0},
            "shopify_payments": {"pct": 0.2, "fixed": 0.0},
            "other": {"pct": 0.05, "fixed": 0.5},
        }
        self.assertEqual(estimate_net_from_revenue(100.0, "paypal", fees), 89.0)
        self.assertEqual(estimate_net_from_revenue(100.0, "shopify_payments", fees), 80.0)
        self.assertEqual(estimate_net_from_revenue(10.0, "other", fees), 9.0)

    def test_apply_clears_estimate_when_ledger_positive(self) -> None:
        fees = {"paypal": {"pct": 0.1, "fixed": 0.0}, "shopify_payments": {"pct": 0.1, "fixed": 0.0}, "other": {"pct": 0.1, "fixed": 0.0}}
        settings = SimpleNamespace(payment_net_estimate=True, payment_net_estimate_fees=fees)
        df = pd.DataFrame(
            [
                {
                    "Revenue": 100.0,
                    "Payment_Net": 95.0,
                    "Payment_Gateway_Names": "paypal",
                },
                {
                    "Revenue": 50.0,
                    "Payment_Net": 0.0,
                    "Payment_Gateway_Names": "paypal",
                },
            ]
        )
        out = apply_payment_net_estimate(df, settings)
        self.assertTrue(np.isnan(out.loc[0, "Payment_Net_Estimate"]))
        self.assertEqual(out.loc[1, "Payment_Net_Estimate"], 45.0)

    def test_apply_disabled_drops_column(self) -> None:
        settings = SimpleNamespace(payment_net_estimate=False, payment_net_estimate_fees=None)
        df = pd.DataFrame([{"Revenue": 1.0, "Payment_Net": 0.0, "Payment_Net_Estimate": 0.99}])
        out = apply_payment_net_estimate(df, settings)
        self.assertNotIn("Payment_Net_Estimate", out.columns)

    def test_order_level_rollup_estimate(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-04-01",
                    "Order": "#1",
                    "Order_ID": 1,
                    "Line_Item_ID": 1,
                    "Product": "A",
                    "Quantity": 1,
                    "Revenue": 10.0,
                    "Product_Cost": 4.0,
                    "Gross_Profit": 6.0,
                    "Payment_Gateway_Names": "paypal",
                    "Payment_Net": 0.0,
                    "Payment_Net_Estimate": 9.0,
                },
                {
                    "Date": "2026-04-01",
                    "Order": "#1",
                    "Order_ID": 1,
                    "Line_Item_ID": 2,
                    "Product": "B",
                    "Quantity": 1,
                    "Revenue": 5.0,
                    "Product_Cost": 2.0,
                    "Gross_Profit": 3.0,
                    "Payment_Gateway_Names": "paypal",
                    "Payment_Net": 0.0,
                    "Payment_Net_Estimate": 9.0,
                },
            ]
        )
        out = order_level_summary(df)
        self.assertEqual(len(out), 1)
        self.assertEqual(out.loc[0, "Payment_Net_Estimate"], 9.0)
        self.assertEqual(out.loc[0, "Payment_Gateway_Names"], "paypal")

    def test_order_level_prefers_nonempty_gateway(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-04-01",
                    "Order": "#1",
                    "Order_ID": 1,
                    "Line_Item_ID": 1,
                    "Product": "A",
                    "Quantity": 1,
                    "Revenue": 10.0,
                    "Product_Cost": 4.0,
                    "Gross_Profit": 6.0,
                    "Payment_Gateway_Names": "",
                    "Payment_Net": 0.0,
                    "Payment_Net_Estimate": 9.0,
                },
                {
                    "Date": "2026-04-01",
                    "Order": "#1",
                    "Order_ID": 1,
                    "Line_Item_ID": 2,
                    "Product": "B",
                    "Quantity": 1,
                    "Revenue": 5.0,
                    "Product_Cost": 2.0,
                    "Gross_Profit": 3.0,
                    "Payment_Gateway_Names": "shopify_payments",
                    "Payment_Net": 0.0,
                    "Payment_Net_Estimate": 4.0,
                },
            ]
        )
        out = order_level_summary(df)
        self.assertEqual(len(out), 1)
        self.assertEqual(out.loc[0, "Payment_Gateway_Names"], "shopify_payments")


if __name__ == "__main__":
    unittest.main()
