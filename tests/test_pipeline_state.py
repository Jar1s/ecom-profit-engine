from __future__ import annotations

from dataclasses import replace as dc_replace
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from pipeline import (
    _load_meta_df,
    _load_orders_db_df,
    _merge_meta_df,
    _merge_meta_rows_with_existing,
    _tracking_candidate_order_ids,
    _updated_at_min_from_state,
)
from pipeline_state import PipelineState, load_pipeline_state, save_pipeline_state


class PipelineStateTests(TestCase):
    def test_load_pipeline_state_defaults_when_sheet_missing(self) -> None:
        settings = SimpleNamespace(pipeline_state_tab="PIPELINE_STATE")
        with patch("pipeline_state.try_read_worksheet_dataframe", return_value=None):
            state = load_pipeline_state(settings)  # type: ignore[arg-type]
        self.assertEqual(state, PipelineState())

    def test_load_pipeline_state_reads_key_values(self) -> None:
        settings = SimpleNamespace(pipeline_state_tab="PIPELINE_STATE")
        df = pd.DataFrame(
            [
                {"key": "schema_version", "value": "2"},
                {"key": "shopify_orders_updated_at_max", "value": "2026-04-19T10:00:00+00:00"},
                {"key": "last_core_sync_at", "value": "2026-04-19T11:00:00+00:00"},
            ]
        )
        with patch("pipeline_state.try_read_worksheet_dataframe", return_value=df):
            state = load_pipeline_state(settings)  # type: ignore[arg-type]
        self.assertEqual(state.schema_version, 2)
        self.assertEqual(state.shopify_orders_updated_at_max, "2026-04-19T10:00:00+00:00")
        self.assertEqual(state.last_core_sync_at, "2026-04-19T11:00:00+00:00")

    def test_save_pipeline_state_writes_key_value_rows(self) -> None:
        settings = SimpleNamespace(pipeline_state_tab="PIPELINE_STATE")
        state = dc_replace(PipelineState(), last_successful_run_kind="core")
        with patch("pipeline_state.replace_worksheet_simple") as repl:
            save_pipeline_state(settings, state)  # type: ignore[arg-type]
        args = repl.call_args[0]
        self.assertEqual(args[1], "PIPELINE_STATE")
        df = args[2]
        self.assertIn("key", df.columns)
        self.assertIn("value", df.columns)
        self.assertEqual(df.loc[df["key"] == "last_successful_run_kind", "value"].iloc[0], "core")

    def test_updated_at_min_with_overlap(self) -> None:
        state = dc_replace(
            PipelineState(),
            shopify_orders_updated_at_max="2026-04-19T12:00:00+00:00",
        )
        out = _updated_at_min_from_state(state, 10)
        self.assertEqual(out, "2026-04-19T11:50:00+00:00")


class TrackingCandidateTests(TestCase):
    def test_tracking_candidates_exclude_delivered(self) -> None:
        settings = SimpleNamespace(tracking_active_lookback_days=30)
        df = pd.DataFrame(
            [
                {"Order_ID": 1, "Date": "2026-04-18", "Delivery_Status": "Delivered", "Shipment_Status": "", "Carrier_Tracking_Status": ""},
                {"Order_ID": 2, "Date": "2026-04-18", "Delivery_Status": "In transit", "Shipment_Status": "in_transit", "Carrier_Tracking_Status": ""},
                {"Order_ID": 3, "Date": "2026-04-18", "Delivery_Status": "", "Shipment_Status": "", "Carrier_Tracking_Status": "Delivered by carrier"},
            ]
        )
        out = _tracking_candidate_order_ids(settings, df)  # type: ignore[arg-type]
        self.assertEqual(out, [2])


class PipelineSheetLoadTests(TestCase):
    def test_load_orders_db_finds_table_header_in_fancy_layout(self) -> None:
        settings = SimpleNamespace()
        raw = pd.DataFrame(
            [
                {
                    "Date": "2026-04-30",
                    "Order": "#1001",
                    "Order_ID": "1001",
                    "Line_Item_ID": "2001",
                    "Product": "Widget",
                    "Quantity": "1",
                    "Revenue": "19.99",
                    "Product_Cost": "9.5",
                    "Gross_Profit": "10.49",
                    "Days_In_Transit": "",
                }
            ]
        )
        with patch("pipeline.try_read_worksheet_dataframe", return_value=raw) as read_df:
            out = _load_orders_db_df(settings)  # type: ignore[arg-type]
        self.assertEqual(read_df.call_args.kwargs["required_headers"], ("Date", "Order_ID", "Line_Item_ID"))
        self.assertEqual(out["Order_ID"].iloc[0], 1001)
        self.assertEqual(out["Line_Item_ID"].iloc[0], 2001)

    def test_load_orders_db_requires_date_for_reporting_reuse(self) -> None:
        settings = SimpleNamespace()
        raw = pd.DataFrame(
            [
                {
                    "Order": "#1001",
                    "Order_ID": "1001",
                    "Line_Item_ID": "2001",
                    "Product": "Widget",
                    "Quantity": "1",
                    "Revenue": "19.99",
                }
            ]
        )
        with patch("pipeline.try_read_worksheet_dataframe", return_value=raw):
            out = _load_orders_db_df(settings)  # type: ignore[arg-type]
        self.assertTrue(out.empty)
        self.assertIn("Date", out.columns)

    def test_load_meta_df_finds_date_header_in_fancy_layout(self) -> None:
        settings = SimpleNamespace()
        raw = pd.DataFrame([{"Date": "2026-04-30", "Ad_Spend_USD": "12.34"}])
        with patch("pipeline.try_read_worksheet_dataframe", return_value=raw) as read_df:
            out = _load_meta_df(settings)  # type: ignore[arg-type]
        self.assertEqual(read_df.call_args.kwargs["required_headers"], ("Date",))
        self.assertEqual(out["Ad_Spend_USD"].iloc[0], 12.34)

    def test_merge_meta_rows_preserves_existing_dates_outside_fresh_range(self) -> None:
        settings = SimpleNamespace(usd_per_local=None, meta_spend_in_usd=True)
        existing = pd.DataFrame(
            [
                {"Date": "2026-01-01", "Ad_Spend_USD": "5.00"},
                {"Date": "2026-04-30", "Ad_Spend_USD": "10.00"},
            ]
        )
        fresh = [{"Date": "2026-04-30", "Ad_Spend": 12.5}, {"Date": "2026-05-01", "Ad_Spend": 7.0}]
        with patch("pipeline._load_meta_df", return_value=existing):
            rows = _merge_meta_rows_with_existing(settings, fresh)  # type: ignore[arg-type]
        by_date = {row["Date"]: row["Ad_Spend"] for row in rows}
        self.assertEqual(by_date["2026-01-01"], 5.0)
        self.assertEqual(by_date["2026-04-30"], 12.5)
        self.assertEqual(by_date["2026-05-01"], 7.0)

    def test_merge_meta_df_drops_duplicate_sheet_headers(self) -> None:
        existing = pd.DataFrame(
            [["2026-04-30", "10.00", "duplicate"]],
            columns=["Date", "Ad_Spend_USD", "Ad_Spend_USD"],
        )
        fresh = pd.DataFrame([{"Date": "2026-05-01", "Ad_Spend_USD": 7.0}])
        out = _merge_meta_df(existing, fresh)
        self.assertEqual(list(out.columns), ["Date", "Ad_Spend_USD"])
        self.assertEqual(len(out), 2)
