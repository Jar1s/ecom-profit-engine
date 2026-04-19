#!/usr/bin/env python3
"""Compare current full build outputs with a selected pipeline mode without writing Sheets."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import load_settings
from costs import load_cost_maps
from pipeline import (
    _normalize_mode,
    _run_core,
    _run_full,
    _run_reporting,
    _run_tracking,
)
from pipeline_state import load_pipeline_state


def _sort_df(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    cols = [c for c in keys if c in df.columns]
    if not cols:
        return df.reset_index(drop=True)
    return df.sort_values(cols, kind="stable").reset_index(drop=True)


def _compare(name: str, left: pd.DataFrame, right: pd.DataFrame, keys: list[str]) -> bool:
    a = _sort_df(left, keys).fillna("")
    b = _sort_df(right, keys).fillna("")
    same = a.equals(b)
    print(f"{name}: {'OK' if same else 'DIFF'} rows={len(a)} vs {len(b)}")
    if not same:
        print(a.head(3).to_dict(orient="records"))
        print(b.head(3).to_dict(orient="records"))
    return same


def main() -> int:
    settings = load_settings()
    mode = _normalize_mode(settings)
    state = load_pipeline_state(settings)
    cost_maps = load_cost_maps(settings)

    baseline = _run_full(settings, cost_maps)
    if mode == "core":
        actual = _run_core(settings, cost_maps, state)
        ok = all(
            [
                _compare("ORDERS_DB", baseline.orders_df, actual.orders_df, ["Line_Item_ID"]),
                _compare("ORDER_LEVEL", baseline.order_df, actual.order_df, ["Order_ID"]),
                _compare("DAILY_SUMMARY", baseline.daily_final, actual.daily_final, ["Date"]),
                _compare("META_DATA", baseline.meta_df, actual.meta_df, ["Date"]),
            ]
        )
    elif mode == "tracking":
        actual = _run_tracking(settings, cost_maps, state)
        ok = all(
            [
                _compare("ORDERS_DB", baseline.orders_df, actual.orders_df, ["Line_Item_ID"]),
                _compare("ORDER_LEVEL", baseline.order_df, actual.order_df, ["Order_ID"]),
                _compare("DAILY_SUMMARY", baseline.daily_final, actual.daily_final, ["Date"]),
            ]
        )
    elif mode == "reporting":
        actual = _run_reporting(settings, cost_maps, state)
        ok = all(
            [
                _compare("META_CAMPAIGNS", baseline.meta_campaign_df, actual.meta_campaign_df, ["Date", "Campaign_ID"]),
                _compare("BOOKKEEPING", baseline.bookkeeping_df, actual.bookkeeping_df, ["Month"]),
            ]
        )
    else:
        print("Set PIPELINE_MODE=core|tracking|reporting for parity comparison.")
        return 1
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
