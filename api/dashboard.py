from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from config import Settings, load_settings
from pipeline import (
    SHEET_BOOKKEEPING,
    SHEET_DAILY,
    SHEET_META_CAMPAIGNS,
    SHEET_ORDER_LEVEL,
    SHEET_PAYOUTS_FEES,
)
from pipeline_state import PipelineState, load_pipeline_state
from sheets import read_dashboard_sheet_tabs

logger = logging.getLogger("ecom_profit_engine.dashboard")


@dataclass(frozen=True)
class DashboardBundle:
    """Sheets needed for dashboard APIs only (not full ORDERS_DB / META_DATA — unused, slow)."""

    settings: Settings
    state: PipelineState
    order_level_df: pd.DataFrame
    daily_df: pd.DataFrame
    meta_campaigns_df: pd.DataFrame
    bookkeeping_df: pd.DataFrame
    payouts_fees_df: pd.DataFrame
    missing_costs_df: pd.DataFrame


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


_bundle_cache_lock = threading.Lock()
_bundle_cache: tuple[float, str, DashboardBundle] | None = None


def clear_dashboard_bundle_cache() -> None:
    """Drop cached dashboard bundle (call after pipeline writes to Sheets so /app sees fresh data)."""
    global _bundle_cache
    with _bundle_cache_lock:
        _bundle_cache = None


def _dashboard_bundle_cache_key(settings: Settings) -> str:
    return f"{settings.google_sheet_id or ''}|{settings.google_sheet_name or ''}"


def _load_dashboard_bundle_uncached(settings: Settings) -> DashboardBundle:
    """
    Load dashboard tabs: one spreadsheet open + parallel tab reads (see sheets.read_dashboard_sheet_tabs).
    Skips ORDERS_DB / META_DATA — unused by /app.
    """
    missing_tab = settings.missing_supplier_costs_tab
    specs: list[tuple[str, tuple[str, ...] | None]] = [
        (SHEET_ORDER_LEVEL, ("Order_ID",)),
        (SHEET_DAILY, ("Date",)),
        (SHEET_META_CAMPAIGNS, ("Date",)),
        (SHEET_BOOKKEEPING, ("Month",)),
        (SHEET_PAYOUTS_FEES, ("Date",)),
        (missing_tab, None),
    ]
    state = _load_state_safe(settings)
    sheets_out = read_dashboard_sheet_tabs(settings, specs)
    (
        order_level_df,
        daily_df,
        meta_campaigns_df,
        bookkeeping_df,
        payouts_fees_df,
        missing_costs_df,
    ) = sheets_out

    return DashboardBundle(
        settings=settings,
        state=state,
        order_level_df=order_level_df,
        daily_df=daily_df,
        meta_campaigns_df=meta_campaigns_df,
        bookkeeping_df=bookkeeping_df,
        payouts_fees_df=payouts_fees_df,
        missing_costs_df=missing_costs_df,
    )


def load_dashboard_bundle(settings: Settings | None = None) -> DashboardBundle:
    """
    Cached dashboard bundle load. Repeated /api/app/* hits reuse recent Sheets reads (short TTL).
    Set DASHBOARD_BUNDLE_CACHE_SECONDS=0 to disable.
    """
    global _bundle_cache
    settings = settings or load_settings()
    try:
        ttl = float(os.getenv("DASHBOARD_BUNDLE_CACHE_SECONDS", "20"))
    except ValueError:
        ttl = 20.0
    key = _dashboard_bundle_cache_key(settings)
    now = time.monotonic()
    if ttl > 0:
        with _bundle_cache_lock:
            if _bundle_cache is not None:
                ts, cached_key, bundle = _bundle_cache
                if cached_key == key and (now - ts) < ttl:
                    return bundle

    bundle = _load_dashboard_bundle_uncached(settings)

    if ttl > 0:
        with _bundle_cache_lock:
            _bundle_cache = (time.monotonic(), key, bundle)

    return bundle


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
    "Profit After Fees": {"mode": "higher_better", "green": 0.0, "amber": -500.0},
    "Payment net 30d": {"mode": "higher_better", "green": 0.0, "amber": -500.0},
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
    bookkeeping = bundle.bookkeeping_df.copy()
    latest_month = None
    latest_operating_income = None
    if not bookkeeping.empty and "Month" in bookkeeping.columns:
        bookkeeping = bookkeeping.sort_values("Month")
        latest = bookkeeping.iloc[-1]
        latest_month = str(latest.get("Month") or "")
        if "Operating_income" in bookkeeping.columns:
            latest_operating_income = _to_numeric(pd.Series([latest.get("Operating_income")])).iloc[0]

    revenue_30 = _to_numeric(daily_recent.get("Revenue", pd.Series(dtype=float))).sum()
    refunds_30 = 0.0
    if not daily_recent.empty and "Refunds_Total" in daily_recent.columns:
        refunds_30 = _to_numeric(daily_recent.get("Refunds_Total", pd.Series(dtype=float))).sum()
    gross_profit_30 = _to_numeric(daily_recent.get("Gross_Profit", pd.Series(dtype=float))).sum()
    ad_spend_30 = _to_numeric(daily_recent.get("Ad_Spend", pd.Series(dtype=float))).sum()
    payouts_recent = _recent_window(bundle.payouts_fees_df, 30)
    payout_fees_30 = _to_numeric(payouts_recent.get("Fee_Amount", pd.Series(dtype=float))).sum()
    payment_net_30 = 0.0
    if not order_level_recent.empty and "Payment_Net" in order_level_recent.columns:
        payment_net_30 = _to_numeric(order_level_recent.get("Payment_Net", pd.Series(dtype=float))).sum()
    orders_30 = _to_numeric(daily_recent.get("Orders_Total", pd.Series(dtype=float))).sum()
    delivered_30 = _to_numeric(daily_recent.get("Orders_Delivered", pd.Series(dtype=float))).sum()
    undelivered_now = 0.0
    if not order_level_recent.empty and "Delivery_Status" in order_level_recent.columns:
        delivery = order_level_recent["Delivery_Status"].astype(str).str.strip().str.lower()
        undelivered_now = float((delivery != "delivered").sum())
    roas_30 = (revenue_30 / ad_spend_30) if ad_spend_30 > 0 else None
    profit_after_fees_30 = gross_profit_30 - ad_spend_30 - payout_fees_30
    last_sync = _latest_timestamp(
        bundle.state.last_core_sync_at,
        bundle.state.last_tracking_sync_at,
        bundle.state.last_reporting_sync_at,
    )

    return [
        {"label": "Revenue 30d", "value": _fmt_money(revenue_30), "meta": "z DAILY_SUMMARY"},
        {
            "label": "Refunds 30d",
            "value": _fmt_money(refunds_30),
            "meta": (
                "z DAILY_SUMMARY (Refunds_Total v USD, rovnako ako Revenue pri USD_PRIMARY)"
                if bundle.settings.daily_summary_usd_primary
                else "z DAILY_SUMMARY (Refunds_Total v mene obchodu)"
            ),
        },
        {"label": "Gross Profit 30d", "value": _fmt_money(gross_profit_30), "meta": "po supplier cost"},
        {"label": "Ad Spend 30d", "value": _fmt_money(ad_spend_30), "meta": "Meta daily spend"},
        {"label": "Payout Fees 30d", "value": _fmt_money(payout_fees_30), "meta": "Shopify payouts fees"},
        {
            "label": "Payment net 30d",
            "value": _fmt_money(payment_net_30),
            "meta": "Shopify Payments ledger net (ORDER_LEVEL); stĺpec Payment_Net_Estimate je len pri PAYMENT_NET_ESTIMATE",
        },
        {"label": "ROAS 30d", "value": _fmt_ratio(roas_30), "meta": "Revenue / Ad Spend"},
        {"label": "Orders 30d", "value": _fmt_int(orders_30), "meta": f"Delivered {_fmt_int(delivered_30)}"},
        {"label": "Undelivered", "value": _fmt_int(undelivered_now), "meta": "aktívne order-level"},
        {
            "label": "Profit After Fees",
            "value": _fmt_money(profit_after_fees_30),
            "meta": "Gross Profit - Ads - Payout fees (30d)",
        },
        {
            "label": "Last Sync",
            "value": _fmt_timestamp_short(last_sync),
            "meta": bundle.state.last_successful_run_kind or "bez state",
        },
        {"label": "Operating Income", "value": _fmt_money(latest_operating_income), "meta": latest_month or "BOOKKEEPING"},
    ]


def run_status_rows(bundle: DashboardBundle) -> list[dict[str, str]]:
    rows = [
        {
            "job": "core",
            "last_run": _fmt_timestamp_short(bundle.state.last_core_sync_at),
            "purpose": "Shopify + supplier + daily Meta + hlavné taby",
        },
        {
            "job": "tracking",
            "last_run": _fmt_timestamp_short(bundle.state.last_tracking_sync_at),
            "purpose": "17TRACK + delivery refresh",
        },
        {
            "job": "reporting",
            "last_run": _fmt_timestamp_short(bundle.state.last_reporting_sync_at),
            "purpose": "META_CAMPAIGNS + BOOKKEEPING",
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
        "Refunds_Total",
        "Net_Revenue_After_Refunds",
        "Gross_Profit_After_Refunds",
        "Payment_Gateway_Names",
        "Payment_Net",
        "Payment_Net_Estimate",
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
        "Refunds_Total",
        "Product_Cost",
        "Gross_Profit",
        "Ad_Spend",
        "Marketing_ROAS",
        "Orders_Total",
        "Orders_Delivered",
        "Orders_Undelivered",
    ]
    return df[[c for c in preferred if c in df.columns]].head(limit)


def marketing_campaign_table(bundle: DashboardBundle, limit: int = 50) -> pd.DataFrame:
    df = bundle.meta_campaigns_df.copy()
    if df.empty:
        return df
    if "Date" in df.columns:
        df = _coerce_date_col(df).sort_values(["Date"], ascending=False)
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    preferred = [
        "Date",
        "Campaign_Name",
        "Campaign_ID",
        "Ad_Spend",
        "Purchase_Value",
        "Purchases",
        "ROAS",
    ]
    return df[[c for c in preferred if c in df.columns]].head(limit)


def bookkeeping_table(bundle: DashboardBundle, limit: int = 24) -> pd.DataFrame:
    df = bundle.bookkeeping_df.copy()
    if df.empty:
        return df
    if "Month" in df.columns:
        df = df.sort_values("Month", ascending=False)
    return df.head(limit)


def payouts_fees_table(bundle: DashboardBundle, limit: int = 120) -> pd.DataFrame:
    df = bundle.payouts_fees_df.copy()
    if df.empty:
        return df
    if "Date" in df.columns:
        df = _coerce_date_col(df).sort_values("Date", ascending=False)
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    preferred = [
        "Date",
        "Payout_ID",
        "Payout_Status",
        "Currency",
        "Transaction_Type",
        "Source_Type",
        "Source_Order_ID",
        "Gross_Amount",
        "Fee_Amount",
        "Net_Amount",
    ]
    return df[[c for c in preferred if c in df.columns]].head(limit)


def missing_costs_table(bundle: DashboardBundle, limit: int = 50) -> pd.DataFrame:
    df = bundle.missing_costs_df.copy()
    if df.empty:
        return df
    return df.head(limit)
