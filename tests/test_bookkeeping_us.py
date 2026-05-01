"""Tests for US bookkeeping monthly rollup."""

from __future__ import annotations

import unittest

import pandas as pd

from bookkeeping_us import bookkeeping_us_monthly, _order_us_row


class TestBookkeepingUS(unittest.TestCase):
    def test_order_us_row_parses_money(self) -> None:
        order = {
            "id": 1,
            "created_at": "2026-03-15T12:00:00-04:00",
            "total_line_items_price": "100.00",
            "total_discounts": "10.00",
            "subtotal_price": "90.00",
            "total_tax": "7.20",
            "total_shipping_price_set": {"shop_money": {"amount": "5.00", "currency_code": "USD"}},
            "refunds": [],
        }
        r = _order_us_row(order)
        self.assertIsNotNone(r)
        self.assertEqual(r["Gross_merchandise"], 100.0)
        self.assertEqual(r["Product_sales_net"], 90.0)
        self.assertEqual(r["Shipping_revenue"], 5.0)
        self.assertEqual(r["Sales_tax_collected"], 7.2)
        self.assertEqual(r["Net_sales"], 95.0)  # 90 + 5 - 0 refunds

    def test_bookkeeping_us_monthly_merges_cogs_and_ads(self) -> None:
        orders = [
            {
                "id": 1,
                "created_at": "2026-01-10T12:00:00Z",
                "total_line_items_price": "200",
                "total_discounts": "0",
                "subtotal_price": "200",
                "total_tax": "0",
                "total_shipping_price_set": {"shop_money": {"amount": "0", "currency_code": "USD"}},
                "refunds": [],
            },
            {
                "id": 2,
                "created_at": "2026-01-20T12:00:00Z",
                "total_line_items_price": "100",
                "total_discounts": "0",
                "subtotal_price": "100",
                "total_tax": "0",
                "total_shipping_price_set": None,
                "refunds": [],
            },
        ]
        orders_df = pd.DataFrame(
            [
                {
                    "Date": "2026-01-10",
                    "Order_ID": 1,
                    "Product_Cost": 80.0,
                },
                {
                    "Date": "2026-01-20",
                    "Order_ID": 2,
                    "Product_Cost": 40.0,
                },
            ]
        )
        daily_final = pd.DataFrame(
            [
                {"Date": "2026-01-05", "Ad_Spend": 10.0},
                {"Date": "2026-01-25", "Ad_Spend": 20.0},
            ]
        )
        out = bookkeeping_us_monthly(orders, orders_df, daily_final)
        self.assertEqual(len(out), 1)
        row = out.iloc[0]
        self.assertEqual(row["Month"], "2026-01")
        self.assertEqual(row["Net_sales"], 300.0)
        self.assertEqual(row["COGS"], 120.0)
        self.assertEqual(row["Gross_profit"], 180.0)
        self.assertEqual(row["Marketing_advertising"], 30.0)
        self.assertEqual(row["Operating_income"], 150.0)
        self.assertEqual(row["Payout_Fees_Total"], 0.0)

    def test_bookkeeping_us_monthly_refund_buckets_and_payout_fees(self) -> None:
        orders = [
            {
                "id": 1,
                "created_at": "2026-01-10T12:00:00Z",
                "total_line_items_price": "100",
                "total_discounts": "0",
                "subtotal_price": "100",
                "total_tax": "0",
                "total_shipping_price_set": {"shop_money": {"amount": "0", "currency_code": "USD"}},
                "refunds": [{"transactions": [{"amount": "100"}]}],
            },
            {
                "id": 2,
                "created_at": "2026-01-11T12:00:00Z",
                "total_line_items_price": "100",
                "total_discounts": "0",
                "subtotal_price": "100",
                "total_tax": "0",
                "total_shipping_price_set": {"shop_money": {"amount": "0", "currency_code": "USD"}},
                "refunds": [{"transactions": [{"amount": "50"}]}],
            },
            {
                "id": 3,
                "created_at": "2026-01-12T12:00:00Z",
                "total_line_items_price": "100",
                "total_discounts": "0",
                "subtotal_price": "100",
                "total_tax": "0",
                "total_shipping_price_set": {"shop_money": {"amount": "0", "currency_code": "USD"}},
                "refunds": [{"transactions": [{"amount": "20"}]}],
            },
        ]
        orders_df = pd.DataFrame(
            [
                {"Date": "2026-01-10", "Order_ID": 1, "Product_Cost": 20.0},
                {"Date": "2026-01-11", "Order_ID": 2, "Product_Cost": 20.0},
                {"Date": "2026-01-12", "Order_ID": 3, "Product_Cost": 20.0},
            ]
        )
        daily_final = pd.DataFrame([{"Date": "2026-01-15", "Ad_Spend": 10.0}])
        payout_monthly = pd.DataFrame([{"Month": "2026-01", "Payout_Fees_Total": 8.5}])
        out = bookkeeping_us_monthly(
            orders,
            orders_df,
            daily_final,
            payout_fees_monthly_df=payout_monthly,
        )
        row = out.iloc[0]
        self.assertEqual(row["Refunds_Full_Count"], 1)
        self.assertEqual(row["Refunds_Half_Count"], 1)
        self.assertEqual(row["Refunds_Other_Count"], 1)
        self.assertEqual(row["Refunds_Full_Amount"], 100.0)
        self.assertEqual(row["Refunds_Half_Amount"], 50.0)
        self.assertEqual(row["Refunds_Other_Amount"], 20.0)
        self.assertEqual(row["Payout_Fees_Total"], 8.5)
        self.assertEqual(
            row["Operating_Income_After_Payout_Fees"],
            row["Operating_income"] - 8.5,
        )


if __name__ == "__main__":
    unittest.main()
