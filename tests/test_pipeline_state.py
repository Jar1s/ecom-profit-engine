from __future__ import annotations

from dataclasses import replace as dc_replace
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from pipeline import (
    _load_meta_df,
    _merge_meta_df,
    _merge_meta_rows_with_existing,
    _meta_rows_from_campaign_rows,
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
        state = dc_replace(PipelineState(), last_successful_run_kind="full")
        with patch("pipeline_state.replace_worksheet_simple") as repl:
            save_pipeline_state(settings, state)  # type: ignore[arg-type]
        args = repl.call_args[0]
        self.assertEqual(args[1], "PIPELINE_STATE")
        df = args[2]
        self.assertIn("key", df.columns)
        self.assertIn("value", df.columns)
        self.assertEqual(df.loc[df["key"] == "last_successful_run_kind", "value"].iloc[0], "full")


class PipelineSheetLoadTests(TestCase):
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

    def test_merge_meta_rows_keeps_existing_usd_amounts_as_api_currency(self) -> None:
        settings = SimpleNamespace(usd_per_local=0.65, meta_spend_in_usd=True)
        existing = pd.DataFrame([{"Date": "2026-01-01", "Ad_Spend_USD": "100.00"}])
        with patch("pipeline._load_meta_df", return_value=existing):
            rows = _merge_meta_rows_with_existing(settings, [])
        self.assertEqual(rows, [{"Date": "2026-01-01", "Ad_Spend": 100.0}])

    def test_meta_rows_from_campaign_rows_sums_daily_campaign_spend(self) -> None:
        rows = _meta_rows_from_campaign_rows(
            [
                {"Date": "2026-05-01", "Campaign_ID": "1", "Ad_Spend": 10.125},
                {"Date": "2026-05-01", "Campaign_ID": "2", "Ad_Spend": "5.335"},
                {"Date": "2026-05-02", "Campaign_ID": "1", "Ad_Spend": 7},
            ]
        )
        self.assertEqual(
            rows,
            [
                {"Date": "2026-05-01", "Ad_Spend": 15.46},
                {"Date": "2026-05-02", "Ad_Spend": 7.0},
            ],
        )

    def test_merge_meta_df_drops_duplicate_sheet_headers(self) -> None:
        existing = pd.DataFrame(
            [["2026-04-30", "10.00", "duplicate"]],
            columns=["Date", "Ad_Spend_USD", "Ad_Spend_USD"],
        )
        fresh = pd.DataFrame([{"Date": "2026-05-01", "Ad_Spend_USD": 7.0}])
        out = _merge_meta_df(existing, fresh)
        self.assertEqual(list(out.columns), ["Date", "Ad_Spend_USD"])
        self.assertEqual(len(out), 2)
