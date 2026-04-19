from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from config import Settings, load_settings
from pipeline import (
    SHEET_BOOKKEEPING,
    SHEET_DAILY,
    SHEET_META_DATA,
    SHEET_META_CAMPAIGNS,
    SHEET_ORDER_LEVEL,
    SHEET_ORDERS_DB,
)
from pipeline_state import PipelineState, load_pipeline_state
import logging
from sheets import try_read_worksheet_dataframe

logger = logging.getLogger("ecom_profit_engine.dashboard")


@dataclass(frozen=True)
class DashboardBundle:
    settings: Settings
    state: PipelineState
    orders_df: pd.DataFrame
    order_level_df: pd.DataFrame
    daily_df: pd.DataFrame
    meta_df: pd.DataFrame
    meta_campaigns_df: pd.DataFrame
    bookkeeping_df: pd.DataFrame
    missing_costs_df: pd.DataFrame


def _load_df(settings: Settings, tab: str, required: tuple[str, ...] | None = None) -> pd.DataFrame:
    df = try_read_worksheet_dataframe(settings, tab, required_headers=required)
    if df is None:
        return pd.DataFrame()
    return df.copy()


def load_dashboard_bundle(settings: Settings | None = None) -> DashboardBundle:
    settings = settings or load_settings()
    try:
        state = load_pipeline_state(settings)
    except Exception as exc:
        logger.warning("Could not load pipeline state for dashboard: %s", exc)
        state = PipelineState(last_error_summary=f"Dashboard state unavailable: {exc}")
    return DashboardBundle(
        settings=settings,
        state=state,
        orders_df=_load_df(settings, SHEET_ORDERS_DB, ("Order_ID", "Line_Item_ID")),
        order_level_df=_load_df(settings, SHEET_ORDER_LEVEL, ("Order_ID",)),
        daily_df=_load_df(settings, SHEET_DAILY, ("Date",)),
        meta_df=_load_df(settings, SHEET_META_DATA, ("Date",)),
        meta_campaigns_df=_load_df(settings, SHEET_META_CAMPAIGNS, ("Date",)),
        bookkeeping_df=_load_df(settings, SHEET_BOOKKEEPING, ("Month",)),
        missing_costs_df=_load_df(settings, settings.missing_supplier_costs_tab),
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
        {"label": "Last Sync", "value": last_sync or "—", "meta": bundle.state.last_successful_run_kind or "bez state"},
        {"label": "Operating Income", "value": _fmt_money(latest_operating_income), "meta": latest_month or "BOOKKEEPING"},
    ]


def run_status_rows(bundle: DashboardBundle) -> list[dict[str, str]]:
    rows = [
        {"job": "core", "last_run": bundle.state.last_core_sync_at or "—", "purpose": "Shopify + supplier + daily Meta + hlavné taby"},
        {"job": "tracking", "last_run": bundle.state.last_tracking_sync_at or "—", "purpose": "17TRACK + delivery refresh"},
        {"job": "reporting", "last_run": bundle.state.last_reporting_sync_at or "—", "purpose": "META_CAMPAIGNS + BOOKKEEPING"},
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


def missing_costs_table(bundle: DashboardBundle, limit: int = 50) -> pd.DataFrame:
    df = bundle.missing_costs_df.copy()
    if df.empty:
        return df
    return df.head(limit)
