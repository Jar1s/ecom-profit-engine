from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from config import Settings, load_settings
from pipeline import SHEET_DAILY, SHEET_ORDER_LEVEL
from pipeline_state import PipelineState, load_pipeline_state
import logging
from sheets import try_read_worksheet_dataframe

logger = logging.getLogger("ecom_profit_engine.dashboard")


@dataclass(frozen=True)
class DashboardBundle:
    """Sheets needed for dashboard APIs only (not full ORDERS_DB / META_DATA — unused, slow)."""

    settings: Settings
    state: PipelineState
    order_level_df: pd.DataFrame
    daily_df: pd.DataFrame
    missing_costs_df: pd.DataFrame


def _load_df(settings: Settings, tab: str, required: tuple[str, ...] | None = None) -> pd.DataFrame:
    df = try_read_worksheet_dataframe(settings, tab, required_headers=required)
    if df is None:
        return pd.DataFrame()
    return df.copy()


def dataframe_to_json_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Serialize worksheet-shaped DataFrame for JSON API (NaN → null)."""
    if df is None or df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso"))


def _load_state_safe(settings: Settings) -> PipelineState:
    try:
        return load_pipeline_state(settings)
    except Exception as exc:
        logger.warning("Could not load pipeline state for dashboard: %s", exc)
        return PipelineState(last_error_summary=f"Dashboard state unavailable: {exc}")


def load_dashboard_bundle(settings: Settings | None = None) -> DashboardBundle:
    """
    Load dashboard tabs in parallel. Each tab was previously opened sequentially (very slow).
    ORDERS_DB and META_DATA are not used by any dashboard view — skip full-sheet reads.
    """
    settings = settings or load_settings()
    missing_tab = settings.missing_supplier_costs_tab
    specs: list[tuple[str, tuple[str, ...] | None]] = [
        (SHEET_ORDER_LEVEL, ("Order_ID",)),
        (SHEET_DAILY, ("Date",)),
    ]
    if missing_tab:
        specs.append((missing_tab, None))
    max_workers = max(
        1,
        min(int(os.getenv("DASHBOARD_SHEET_READ_CONCURRENCY", "10")), 16),
    )

    def _load_one(spec: tuple[str, tuple[str, ...] | None]) -> pd.DataFrame:
        tab, req = spec
        return _load_df(settings, tab, required=req)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        f_state = ex.submit(_load_state_safe, settings)
        futures = [ex.submit(_load_one, s) for s in specs]
        state = f_state.result()
        results = [f.result() for f in futures]
        order_level_df = results[0]
        daily_df = results[1]
        missing_costs_df = results[2] if missing_tab else pd.DataFrame()

    return DashboardBundle(
        settings=settings,
        state=state,
        order_level_df=order_level_df,
        daily_df=daily_df,
        missing_costs_df=missing_costs_df,
    )


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _coerce_date_col(df: pd.DataFrame, col: str = "Date") -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return df
    out = df.copy()
    out[col] = pd.to_datetime(out[col], errors="coerce")
    return out.dropna(subset=[col])


def _fmt_money(value: float | int | None) -> str:
    if value is None:
        return "—"
    return f"{float(value):,.2f}".replace(",", " ")


def _fmt_int(value: float | int | None) -> str:
    if value is None:
        return "—"
    return f"{int(round(float(value))):,}".replace(",", " ")


def _fmt_ratio(value: float | int | None) -> str:
    if value is None:
        return "—"
    return f"{float(value):.2f}x"


def _fmt_timestamp_short(value: str | None) -> str:
    if not value:
        return "—"
    s = str(value).strip()
    if not s:
        return "—"
    try:
        parsed = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return parsed.strftime("%d.%m %H:%M")
    except Exception:
        return s[:16] if len(s) > 16 else s


def _recent_window(df: pd.DataFrame, days: int = 30) -> pd.DataFrame:
    if df.empty or "Date" not in df.columns:
        return df
    d = _coerce_date_col(df, "Date")
    if d.empty:
        return d
    cutoff = d["Date"].max() - pd.Timedelta(days=days - 1)
    return d[d["Date"] >= cutoff].copy()


def _latest_timestamp(*values: str) -> str | None:
    valid = [v for v in values if str(v or "").strip()]
    if not valid:
        return None
    return max(valid)


_EXEC_THRESH_DEFAULTS: dict[str, dict[str, Any]] = {
    "ROAS 30d": {"mode": "higher_better", "green": 2.0, "amber": 1.4},
    "Gross Margin": {"mode": "higher_better", "green": 35.0, "amber": 25.0},
    "Ad Share": {"mode": "lower_better", "green": 20.0, "amber": 30.0},
    "Delivery Rate": {"mode": "higher_better", "green": 85.0, "amber": 70.0},
    "Undelivered": {"mode": "lower_better", "green": 15.0, "amber": 30.0},
    "Undelivered Now": {"mode": "lower_better", "green": 15.0, "amber": 30.0},
    "Undelivered 30d": {"mode": "lower_better", "green": 60.0, "amber": 120.0},
    "Avg Transit Days": {"mode": "lower_better", "green": 5.0, "amber": 8.0},
    "Profit After Ads": {"mode": "higher_better", "green": 0.0, "amber": -500.0},
}


def _merge_threshold_row(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any] | None:
    mode = patch.get("mode", base.get("mode"))
    if mode not in ("higher_better", "lower_better"):
        mode = base.get("mode")
    if mode not in ("higher_better", "lower_better"):
        return None
    green = patch.get("green", base.get("green"))
    amber = patch.get("amber", base.get("amber"))
    try:
        g = float(green)
        a = float(amber)
    except (TypeError, ValueError):
        return None
    return {"mode": str(mode), "green": g, "amber": a}


def dashboard_executive_thresholds() -> dict[str, dict[str, Any]]:
    """
    Traffic-light thresholds for /app dashboard (executive KPIs).

    Override via env ``DASHBOARD_EXECUTIVE_THRESHOLDS_JSON`` — JSON object keyed by metric label,
    values are partial objects with optional ``mode``, ``green``, ``amber`` (merged onto defaults).
    """
    out: dict[str, dict[str, Any]] = {k: dict(v) for k, v in _EXEC_THRESH_DEFAULTS.items()}
    raw = (os.getenv("DASHBOARD_EXECUTIVE_THRESHOLDS_JSON") or "").strip()
    if not raw:
        return out
    try:
        overrides = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("Invalid DASHBOARD_EXECUTIVE_THRESHOLDS_JSON: %s", exc)
        return out
    if not isinstance(overrides, dict):
        return out
    for key, patch in overrides.items():
        if not isinstance(key, str) or key not in out:
            continue
        if not isinstance(patch, dict):
            continue
        merged = _merge_threshold_row(out[key], patch)
        if merged:
            out[key] = merged
    return out


def summary_cards(bundle: DashboardBundle) -> list[dict[str, str]]:
    daily_recent = _recent_window(bundle.daily_df, 30)
    order_level_recent = _recent_window(bundle.order_level_df, 30)

    revenue_30 = _to_numeric(daily_recent.get("Revenue", pd.Series(dtype=float))).sum()
    gross_profit_30 = _to_numeric(daily_recent.get("Gross_Profit", pd.Series(dtype=float))).sum()
    ad_spend_30 = _to_numeric(daily_recent.get("Ad_Spend", pd.Series(dtype=float))).sum()
    orders_30 = _to_numeric(daily_recent.get("Orders_Total", pd.Series(dtype=float))).sum()
    delivered_30 = _to_numeric(daily_recent.get("Orders_Delivered", pd.Series(dtype=float))).sum()
    undelivered_now = 0.0
    if not order_level_recent.empty and "Delivery_Status" in order_level_recent.columns:
        delivery = order_level_recent["Delivery_Status"].astype(str).str.strip().str.lower()
        undelivered_now = float((delivery != "delivered").sum())
    roas_30 = (revenue_30 / ad_spend_30) if ad_spend_30 > 0 else None
    last_sync = _latest_timestamp(
        bundle.state.last_core_sync_at,
        bundle.state.last_tracking_sync_at,
        bundle.state.last_reporting_sync_at,
    )

    return [
        {"label": "Revenue 30d", "value": _fmt_money(revenue_30), "meta": "z DAILY_SUMMARY"},
        {"label": "Gross Profit 30d", "value": _fmt_money(gross_profit_30), "meta": "po supplier cost"},
        {"label": "Ad Spend 30d", "value": _fmt_money(ad_spend_30), "meta": "Meta daily spend"},
        {"label": "ROAS 30d", "value": _fmt_ratio(roas_30), "meta": "Revenue / Ad Spend"},
        {"label": "Orders 30d", "value": _fmt_int(orders_30), "meta": f"Delivered {_fmt_int(delivered_30)}"},
        {"label": "Undelivered", "value": _fmt_int(undelivered_now), "meta": "aktívne order-level"},
        {
            "label": "Last Sync",
            "value": _fmt_timestamp_short(last_sync),
            "meta": bundle.state.last_successful_run_kind or "pipeline",
        },
    ]


def run_status_rows(bundle: DashboardBundle) -> list[dict[str, str]]:
    rows = [
        {
            "job": "pipeline",
            "last_run": _fmt_timestamp_short(bundle.state.last_core_sync_at),
            "purpose": "Shopify orders + Meta daily spend + supplier costs → Sheets",
        },
    ]
    if bundle.state.last_error_summary:
        rows.append({"job": "last_error", "last_run": "recent", "purpose": bundle.state.last_error_summary})
    return rows


def recent_orders_table(bundle: DashboardBundle, limit: int = 30) -> pd.DataFrame:
    df = bundle.order_level_df.copy()
    if df.empty:
        return df
    if "Date" in df.columns:
        df = _coerce_date_col(df).sort_values("Date", ascending=False)
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    preferred = [
        "Date",
        "Order",
        "Order_ID",
        "Revenue",
        "Product_Cost",
        "Gross_Profit",
        "Delivery_Status",
        "Shipment_Status",
        "Carrier_Tracking_Status",
        "Days_In_Transit",
        "Tracking_Numbers",
    ]
    cols = [c for c in preferred if c in df.columns]
    return df[cols].head(limit)


def recent_daily_table(bundle: DashboardBundle, limit: int = 30) -> pd.DataFrame:
    df = bundle.daily_df.copy()
    if df.empty:
        return df
    df = _coerce_date_col(df).sort_values("Date", ascending=False)
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    preferred = [
        "Date",
        "Revenue",
        "Product_Cost",
        "Gross_Profit",
        "Ad_Spend",
        "Marketing_ROAS",
        "Orders_Total",
        "Orders_Delivered",
        "Orders_Undelivered",
    ]
    return df[[c for c in preferred if c in df.columns]].head(limit)


def missing_costs_table(bundle: DashboardBundle, limit: int = 50) -> pd.DataFrame:
    df = bundle.missing_costs_df.copy()
    if df.empty:
        return df
    return df.head(limit)
