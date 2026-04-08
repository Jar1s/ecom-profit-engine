"""Tests for cost join and aggregations."""

import unittest

import pandas as pd

from costs import CostMaps, build_product_lineage_index
from normalize import normalize_product_name
from transform import (
    daily_summary_from_orders,
    daily_summary_usd_primary,
    enrich_line_items,
    enrich_meta_usd_columns,
    merge_daily_with_meta,
    meta_rows_for_daily_merge,
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
        cost_maps = CostMaps(by_product={"widget": 15.0}, by_sku={})
        df = enrich_line_items(rows, cost_maps)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.loc[0, "Product_Cost"], 30.0)
        self.assertEqual(df.loc[0, "Gross_Profit"], 10.0)

    def test_enrich_line_items_matches_supplier_sku(self) -> None:
        rows = [
            {
                "Date": "2026-04-01",
                "Order": "#1001",
                "Order_ID": 1,
                "Line_Item_ID": 10,
                "Product": "Short title from Shopify",
                "SKU": "FCGP42825.27",
                "Quantity": 1,
                "Revenue": 77.95,
            }
        ]
        cost_maps = CostMaps(
            by_product={},
            by_sku={"FCGP42825.27": 22.9},
        )
        df = enrich_line_items(rows, cost_maps)
        self.assertEqual(df.loc[0, "Product_Cost"], 22.9)

    def test_enrich_line_items_sku_prefix_same_model_other_color(self) -> None:
        """ITEM_CATALOG style: one wholesale row for FCGP42825 covers all FCGP42825.* variants."""
        rows = [
            {
                "Date": "2026-04-01",
                "Order": "#1",
                "Order_ID": 1,
                "Line_Item_ID": 10,
                "Product": "Pullover - Navy",
                "SKU": "FCGP42825.32",
                "Quantity": 1,
                "Revenue": 80.0,
            }
        ]
        cost_maps = CostMaps(
            by_product={},
            by_sku={},
            sku_prefix_rules=(("FCGP42825", 22.9),),
            by_product_lineage={},
        )
        df = enrich_line_items(rows, cost_maps)
        self.assertEqual(df.loc[0, "Product_Cost"], 22.9)

    def test_enrich_line_items_single_item_order_from_billdetail(self) -> None:
        """One Shopify line + BillDetail row with only that order's single segment."""
        rows = [
            {
                "Date": "2026-04-01",
                "Order": "#1053",
                "Order_ID": 1,
                "Line_Item_ID": 10,
                "Product": "Marketing title differs from supplier",
                "SKU": "",
                "Quantity": 1,
                "Revenue": 99.0,
            }
        ]
        cost_maps = CostMaps(
            by_product={},
            by_sku={},
            by_order_single={"1053": 24.8},
        )
        df = enrich_line_items(rows, cost_maps)
        self.assertEqual(df.loc[0, "Product_Cost"], 24.8)

    def test_enrich_line_items_same_model_other_color_title(self) -> None:
        """Import má len „beige“ variant; Shopify predá „navy“ — spoločný základ názvu."""
        rows = [
            {
                "Date": "2026-04-01",
                "Order": "#1",
                "Order_ID": 1,
                "Line_Item_ID": 10,
                "Product": "Livia | Oversize-Poncho-Pullover - Navy / L",
                "SKU": "",
                "Quantity": 1,
                "Revenue": 80.0,
            }
        ]
        bp = {
            normalize_product_name(
                "Livia | Oversize-Poncho-Pullover - Beige / L"
            ): 22.9,
        }
        cost_maps = CostMaps(
            by_product=bp,
            by_sku={},
            by_product_lineage=build_product_lineage_index(bp),
        )
        df = enrich_line_items(rows, cost_maps)
        self.assertEqual(df.loc[0, "Product_Cost"], 22.9)

    def test_enrich_line_items_learned_from_orders_sheet(self) -> None:
        rows = [
            {
                "Date": "2026-04-01",
                "Order": "#1",
                "Order_ID": 1,
                "Line_Item_ID": 10,
                "Product": "Mystery product",
                "SKU": "SKU-99",
                "Quantity": 1,
                "Revenue": 50.0,
            }
        ]
        cost_maps = CostMaps(
            by_product={},
            by_sku={},
            learned_by_product_sku={("mystery product", "SKU-99"): 12.34},
            by_product_lineage={},
        )
        df = enrich_line_items(rows, cost_maps)
        self.assertEqual(df.loc[0, "Product_Cost"], 12.34)

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

    def test_enrich_meta_usd_columns_already_usd(self) -> None:
        df = pd.DataFrame([{"Date": "2026-04-01", "Ad_Spend": 100.0}])
        out = enrich_meta_usd_columns(
            df, usd_per_local=0.65, meta_spend_in_usd=True
        )
        self.assertEqual(out.loc[0, "Ad_Spend_USD"], 100.0)

    def test_enrich_meta_usd_columns_shop_currency(self) -> None:
        df = pd.DataFrame([{"Date": "2026-04-01", "Ad_Spend": 100.0}])
        out = enrich_meta_usd_columns(
            df, usd_per_local=0.65, meta_spend_in_usd=False
        )
        self.assertEqual(out.loc[0, "Ad_Spend_USD"], 65.0)

    def test_meta_rows_for_daily_merge_usd_to_aud(self) -> None:
        rows = [{"Date": "2026-04-01", "Ad_Spend": 65.0}]
        merged = meta_rows_for_daily_merge(
            rows, meta_spend_in_usd=True, usd_per_local=0.65
        )
        self.assertEqual(merged[0]["Ad_Spend"], 100.0)

    def test_daily_summary_usd_primary(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-04-01",
                    "Revenue": 100.0,
                    "Product_Cost": 40.0,
                    "Gross_Profit": 60.0,
                    "Ad_Spend": 25.0,
                    "Revenue_USD": 65.0,
                    "Product_Cost_USD": 26.0,
                    "Gross_Profit_USD": 39.0,
                    "Ad_Spend_USD": 16.25,
                    "Marketing_ROAS": 4.0,
                }
            ]
        )
        out = daily_summary_usd_primary(df)
        self.assertNotIn("Revenue_USD", out.columns)
        self.assertEqual(out.loc[0, "Revenue"], 65.0)
        self.assertEqual(out.loc[0, "Product_Cost"], 26.0)
        self.assertEqual(out.loc[0, "Net_Profit"], 22.75)
        self.assertEqual(out.loc[0, "Marketing_ROAS"], round(65.0 / 16.25, 4))


if __name__ == "__main__":
    unittest.main()
