"""Tests for cost join and aggregations."""

import unittest

import pandas as pd

from transform import (
    daily_summary_from_orders,
    enrich_line_items,
    merge_daily_with_meta,
    order_level_summary,
)


class TestTransform(unittest.TestCase):
    def test_enrich_line_items(self) -> None:
        rows = [
            {
                "Date": "2026-04-01",
                "Order": "#1001",
                "Order_ID": 1,
                "Line_Item_ID": 10,
                "Product": "Widget",
                "Quantity": 2,
                "Revenue": 40.0,
            }
        ]
        cost_map = {"widget": 15.0}
        df = enrich_line_items(rows, cost_map)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.loc[0, "Product_Cost"], 30.0)
        self.assertEqual(df.loc[0, "Gross_Profit"], 10.0)

    def test_order_level_summary(self) -> None:
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
                },
            ]
        )
        out = order_level_summary(df)
        self.assertEqual(len(out), 1)
        self.assertEqual(out.loc[0, "Revenue"], 15.0)
        self.assertEqual(out.loc[0, "Product_Cost"], 6.0)
        self.assertEqual(out.loc[0, "Gross_Profit"], 9.0)

    def test_merge_daily_with_meta_roas(self) -> None:
        daily = pd.DataFrame(
            [
                {"Date": "2026-04-01", "Revenue": 100.0, "Product_Cost": 40.0, "Gross_Profit": 60.0},
            ]
        )
        meta = [{"Date": "2026-04-01", "Ad_Spend": 25.0}]
        merged = merge_daily_with_meta(daily, meta)
        self.assertEqual(merged.loc[0, "Marketing_ROAS"], 4.0)


if __name__ == "__main__":
    unittest.main()
