"""Ecom Profit Engine — fetch Shopify + Meta + CSV costs, write Google Sheets."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace as dc_replace
from datetime import date, datetime, timedelta, timezone
import logging
import os
import sys
import time
from typing import Any

import pandas as pd

from bookkeeping_us import bookkeeping_us_monthly
from config import Settings, load_settings
from costs import load_cost_maps
from data_quality import build_missing_supplier_costs_report, log_missing_supplier_costs
from meta_ads import fetch_meta_campaign_insights, fetch_meta_daily_spend
from pipeline_state import PipelineState, load_pipeline_state, save_pipeline_state, utc_now_iso
from sheets import (
    pause_between_sheet_uploads,
    replace_worksheet_simple,
    try_read_worksheet_dataframe,
    upload_dataframe,
)
from shopify_auth import log_shopify_auth_config
from shopify_client import fetch_all_orders, fetch_orders_and_line_rows, fetch_orders_by_ids
from transform import (
    daily_summary_from_orders,
    daily_summary_usd_primary,
    enrich_line_items,
    enrich_meta_usd_columns,
    enrich_usd_columns,
    merge_daily_with_meta,
    meta_rows_for_daily_merge,
    order_level_summary,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("ecom_profit_engine")

SHEET_ORDERS_DB = os.getenv("SHEET_TAB_ORDERS_DB", "ORDERS_DB").strip()
SHEET_ORDER_LEVEL = os.getenv("SHEET_TAB_ORDER_LEVEL", "ORDER_LEVEL").strip()
SHEET_META_DATA = os.getenv("SHEET_TAB_META_DATA", "META_DATA").strip()
SHEET_META_CAMPAIGNS = os.getenv("SHEET_TAB_META_CAMPAIGNS", "META_CAMPAIGNS").strip()
SHEET_DAILY = os.getenv("SHEET_TAB_DAILY_SUMMARY", "DAILY_SUMMARY").strip()
SHEET_BOOKKEEPING = os.getenv("SHEET_TAB_BOOKKEEPING", "BOOKKEEPING").strip()

_META_CAMPAIGN_COLUMNS = [
    "Date",
    "Campaign_ID",
    "Campaign_Name",
    "Ad_Spend",
    "Impressions",
    "Clicks",
    "CPM",
    "CPC_All",
    "CPC_Link",
    "CTR_All_pct",
    "CTR_Link_pct",
    "Adds_to_Cart",
    "Checkouts_Initiated",
    "Purchases",
    "Purchase_Value",
]

_ORDERS_DB_EMPTY_COLUMNS = [
    "Date",
    "Order",
    "Order_ID",
    "Fulfillment_Status",
    "Shipment_Status",
    "Delivery_Status",
    "Tracking_Numbers",
    "Tracking_Companies",
    "Carrier_Tracking_Status",
    "Shipped_Date",
    "Days_In_Transit",
    "Line_Item_ID",
    "Product",
    "SKU",
    "Quantity",
    "Revenue",
    "Product_Cost",
    "Gross_Profit",
]


@dataclass(frozen=True)
class PipelineArtifacts:
    orders_df: pd.DataFrame
    order_df: pd.DataFrame
    daily_final: pd.DataFrame
    meta_df: pd.DataFrame
    meta_campaign_df: pd.DataFrame
    bookkeeping_df: pd.DataFrame
    missing_cost_df: pd.DataFrame
    shopify_orders: list[dict[str, Any]]
    meta_rows: list[dict[str, Any]]
    shopify_updated_at_max: str


def _timed(phase: str, fn):
    started = time.perf_counter()
    result = fn()
    logger.info("timing_phase=%s seconds=%.2f", phase, time.perf_counter() - started)
    return result


def _runtime_budget_seconds() -> float:
    """
    Soft guard against hard platform timeout (e.g. Vercel 300s).
    PIPELINE_RUNTIME_BUDGET_SECONDS:
      - <= 0: disabled
      - unset + Vercel runtime: defaults to 285s
      - unset outside Vercel: disabled
    """
    raw = (os.getenv("PIPELINE_RUNTIME_BUDGET_SECONDS") or "").strip()
    if raw:
        try:
            v = float(raw)
        except ValueError:
            logger.warning("Invalid PIPELINE_RUNTIME_BUDGET_SECONDS=%r; runtime guard disabled", raw)
            return 0.0
        return v if v > 0 else 0.0
    if (os.getenv("VERCEL") or "").strip() == "1":
        return 285.0
    return 0.0


def _check_runtime_budget(
    started_total: float,
    *,
    step: str,
    estimated_seconds: float = 0.0,
) -> None:
    budget = _runtime_budget_seconds()
    if budget <= 0:
        return
    elapsed = time.perf_counter() - started_total
    remaining = budget - elapsed
    if remaining <= max(0.0, estimated_seconds):
        raise RuntimeError(
            "runtime budget reached before step="
            f"{step} (elapsed={elapsed:.1f}s, budget={budget:.1f}s, remaining={remaining:.1f}s). "
            "Use PIPELINE_MODE=core/tracking/reporting, disable heavy sheets formatting "
            "(SHEETS_FANCY_LAYOUT=0, SHEETS_CONDITIONAL_FORMAT=0), or increase function timeout."
        )


def _fetch_meta_daily(settings: Settings) -> list[dict[str, Any]]:
    logger.info("Fetching Meta Ads spend …")
    return _timed("meta_fetch", lambda: fetch_meta_daily_spend(settings))


def _fetch_meta_campaigns(settings: Settings) -> list[dict[str, Any]]:
    logger.info("Fetching Meta campaign insights (spend + conversions) …")
    return _timed("meta_campaign_fetch", lambda: fetch_meta_campaign_insights(settings))


def _fetch_meta_daily_safe(settings: Settings) -> list[dict[str, Any]]:
    try:
        return _fetch_meta_daily(settings)
    except Exception as exc:
        if not settings.meta_continue_on_error:
            raise
        logger.warning(
            "Meta daily fetch failed, continuing with empty spend (META_CONTINUE_ON_ERROR=1): %s",
            exc,
        )
        return []


def _fetch_meta_campaigns_safe(settings: Settings) -> list[dict[str, Any]]:
    try:
        return _fetch_meta_campaigns(settings)
    except Exception as exc:
        if not settings.meta_continue_on_error:
            raise
        logger.warning(
            "Meta campaign fetch failed, continuing with empty campaign data (META_CONTINUE_ON_ERROR=1): %s",
            exc,
        )
        return []


def _fetch_shopify_full(settings: Settings) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    logger.info("Fetching Shopify orders …")
    return _timed("shopify_fetch", lambda: fetch_orders_and_line_rows(settings))


def _fetch_shopify_incremental(
    settings: Settings,
    *,
    updated_at_min: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    logger.info("Fetching Shopify orders …")
    return _timed(
        "shopify_fetch",
        lambda: fetch_orders_and_line_rows(settings, updated_at_min=updated_at_min),
    )


def _normalize_mode(settings: Settings) -> str:
    mode = settings.pipeline_mode.strip().lower()
    return mode if mode in {"auto", "full", "core", "tracking", "reporting"} else "auto"


def _parse_iso_dt(value: str) -> datetime | None:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_sync_stale(last_sync_iso: str, *, minutes: int) -> bool:
    dt = _parse_iso_dt(last_sync_iso)
    if dt is None:
        return True
    return (datetime.now(timezone.utc) - dt) >= timedelta(minutes=max(1, minutes))


def _choose_auto_mode(state: PipelineState) -> str:
    """
    Autonomous rotation that avoids heavy full runs:
    - core: frequent refresh (default fallback)
    - tracking: less frequent shipment status refresh
    - reporting: heavy bookkeeping/campaign refresh roughly daily
    """
    if _is_sync_stale(state.last_reporting_sync_at, minutes=24 * 60):
        return "reporting"
    if _is_sync_stale(state.last_tracking_sync_at, minutes=120):
        return "tracking"
    if _is_sync_stale(state.last_core_sync_at, minutes=30):
        return "core"
    return "core"


def _updated_at_min_from_state(state: PipelineState, overlap_minutes: int) -> str | None:
    last = _parse_iso_dt(state.shopify_orders_updated_at_max)
    if last is None:
        return None
    out = last - timedelta(minutes=max(0, overlap_minutes))
    return out.isoformat()


def _extract_shopify_updated_at_max(orders: list[dict[str, Any]]) -> str:
    best: datetime | None = None
    for order in orders:
        dt = _parse_iso_dt(str(order.get("updated_at") or ""))
        if dt is not None and (best is None or dt > best):
            best = dt
    return best.isoformat() if best is not None else ""


def _empty_orders_df() -> pd.DataFrame:
    return pd.DataFrame(columns=_ORDERS_DB_EMPTY_COLUMNS)


def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)
    return out


def _load_orders_db_df(settings: Settings) -> pd.DataFrame:
    df = try_read_worksheet_dataframe(
        settings,
        SHEET_ORDERS_DB,
        required_headers=("Order_ID", "Line_Item_ID"),
    )
    if df is None or df.empty:
        return _empty_orders_df()
    out = df.copy()
    out = _coerce_numeric(
        out,
        ["Order_ID", "Line_Item_ID", "Quantity", "Revenue", "Product_Cost", "Gross_Profit", "Days_In_Transit"],
    )
    if "Order_ID" in out.columns:
        out["Order_ID"] = out["Order_ID"].astype("Int64")
    if "Line_Item_ID" in out.columns:
        out["Line_Item_ID"] = out["Line_Item_ID"].astype("Int64")
    if "Quantity" in out.columns:
        out["Quantity"] = out["Quantity"].astype(float)
    if "Date" in out.columns:
        out["Date"] = out["Date"].astype(str)
    return out


def _load_meta_df(settings: Settings) -> pd.DataFrame:
    df = try_read_worksheet_dataframe(
        settings,
        SHEET_META_DATA,
        required_headers=("Date",),
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["Date", "Ad_Spend_USD"])
    out = df.copy()
    for col in ("Ad_Spend_USD", "Ad_Spend"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    if "Date" in out.columns:
        out["Date"] = out["Date"].astype(str)
    return out


def _merge_orders_df(existing: pd.DataFrame, changed: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return changed.copy()
    if changed.empty:
        return existing.copy()
    changed_order_ids = set(pd.to_numeric(changed["Order_ID"], errors="coerce").dropna().astype(int).tolist())
    out = existing.copy()
    if changed_order_ids and "Order_ID" in out.columns:
        keep_mask = ~pd.to_numeric(out["Order_ID"], errors="coerce").fillna(-1).astype(int).isin(changed_order_ids)
        out = out.loc[keep_mask].copy()
    out = pd.concat([out, changed], ignore_index=True, sort=False)
    if "Date" in out.columns and "Order" in out.columns:
        out = out.sort_values(["Date", "Order"], kind="stable").reset_index(drop=True)
    return out


def _merge_meta_df(existing: pd.DataFrame, fresh: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return fresh.copy()
    if fresh.empty:
        return existing.copy()
    out = existing.copy()
    if "Date" in fresh.columns and "Date" in out.columns:
        out = out.loc[~out["Date"].astype(str).isin(fresh["Date"].astype(str))].copy()
    out = pd.concat([out, fresh], ignore_index=True, sort=False)
    return out.sort_values("Date", kind="stable").reset_index(drop=True)


def _meta_frame_to_rows(meta_df: pd.DataFrame, settings: Settings) -> list[dict[str, Any]]:
    if meta_df.empty:
        return []
    if "Ad_Spend" in meta_df.columns:
        src = meta_df[["Date", "Ad_Spend"]].copy()
    elif "Ad_Spend_USD" in meta_df.columns:
        src = meta_df[["Date", "Ad_Spend_USD"]].rename(columns={"Ad_Spend_USD": "Ad_Spend"}).copy()
        if settings.meta_spend_in_usd and settings.usd_per_local:
            src["Ad_Spend"] = src["Ad_Spend"].astype(float) / float(settings.usd_per_local)
    else:
        return []
    src["Date"] = src["Date"].astype(str)
    src["Ad_Spend"] = pd.to_numeric(src["Ad_Spend"], errors="coerce").fillna(0.0).round(2)
    return src.to_dict(orient="records")


def _merge_meta_rows_with_existing(settings: Settings, fresh_meta_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Preserve historical META_DATA rows outside the current API range.

    Meta fetches are range-based. A short lookback should update overlapping dates but must not
    delete older spend rows that are still needed for DAILY_SUMMARY / bookkeeping history.
    """
    existing = _load_meta_df(settings)
    fresh = _build_meta_df(fresh_meta_rows, settings)
    merged = _merge_meta_df(existing, fresh)
    return _meta_frame_to_rows(merged, settings)


def _final_tracking_row(row: pd.Series) -> bool:
    delivery = str(row.get("Delivery_Status") or "").strip().lower()
    shipment = str(row.get("Shipment_Status") or "").strip().lower()
    carrier = str(row.get("Carrier_Tracking_Status") or "").strip().lower()
    if delivery == "delivered":
        return True
    if shipment in {"delivered", "canceled", "failure"}:
        return True
    return "delivered" in carrier


def _tracking_candidate_order_ids(settings: Settings, orders_df: pd.DataFrame) -> list[int]:
    if orders_df.empty or "Order_ID" not in orders_df.columns:
        return []
    df = orders_df.copy()
    if "Date" in df.columns:
        dt = pd.to_datetime(df["Date"], errors="coerce")
        cutoff = pd.Timestamp(date.today() - timedelta(days=settings.tracking_active_lookback_days))
        df = df.loc[dt.isna() | (dt >= cutoff)].copy()
    df = df.loc[df.apply(lambda row: not _final_tracking_row(row), axis=1)].copy()
    if df.empty:
        return []
    ids = pd.to_numeric(df["Order_ID"], errors="coerce").dropna().astype(int).drop_duplicates()
    return ids.tolist()


def _settings_for_core_mode(settings: Settings) -> Settings:
    return dc_replace(
        settings,
        meta_campaign_insights=False,
        track17_api_key=None,
        shopify_fulfillment_enrich=False,
        shopify_graphql_fulfillment_verify=False,
    )


def _settings_for_tracking_mode(settings: Settings) -> Settings:
    return dc_replace(
        settings,
        meta_campaign_insights=False,
    )


def _settings_for_reporting_mode(settings: Settings) -> Settings:
    return dc_replace(
        settings,
        track17_api_key=None,
        shopify_fulfillment_enrich=False,
        shopify_graphql_fulfillment_verify=False,
    )


def _build_meta_df(meta_rows: list[dict[str, Any]], settings: Settings) -> pd.DataFrame:
    meta_df_all = enrich_meta_usd_columns(
        pd.DataFrame(meta_rows),
        usd_per_local=settings.usd_per_local,
        meta_spend_in_usd=settings.meta_spend_in_usd,
    )
    if "Ad_Spend_USD" in meta_df_all.columns:
        return meta_df_all[["Date", "Ad_Spend_USD"]].copy()
    return meta_df_all[["Date"]].copy() if "Date" in meta_df_all.columns else meta_df_all.copy()


def _build_artifacts(
    settings: Settings,
    cost_maps,
    *,
    line_rows: list[dict[str, Any]],
    shopify_orders: list[dict[str, Any]],
    meta_rows: list[dict[str, Any]],
    meta_campaign_rows: list[dict[str, Any]] | None,
) -> PipelineArtifacts:
    orders_df = _timed(
        "enrich",
        lambda: enrich_usd_columns(enrich_line_items(line_rows, cost_maps), settings.usd_per_local),
    )
    order_df = _timed(
        "order_level",
        lambda: enrich_usd_columns(order_level_summary(orders_df), settings.usd_per_local),
    )
    daily_df = _timed("daily", lambda: daily_summary_from_orders(orders_df))
    meta_df = _timed("meta_transform", lambda: _build_meta_df(meta_rows, settings))
    meta_campaign_df = pd.DataFrame()
    if meta_campaign_rows is not None:
        meta_campaign_df = _timed(
            "meta_campaign_transform",
            lambda: enrich_meta_usd_columns(
                pd.DataFrame(meta_campaign_rows) if meta_campaign_rows else pd.DataFrame(columns=_META_CAMPAIGN_COLUMNS),
                usd_per_local=settings.usd_per_local,
                meta_spend_in_usd=settings.meta_spend_in_usd,
            ),
        )
    if settings.meta_spend_in_usd and not settings.usd_per_local:
        logger.warning(
            "META_SPEND_IN_USD=1 but USD_PER_LOCAL_UNIT is unset: "
            "daily merge may mix Meta USD with Shopify shop currency for ROAS."
        )
    logger.info("Merging daily summary with Meta spend …")
    meta_for_merge = meta_rows_for_daily_merge(
        meta_rows,
        meta_spend_in_usd=settings.meta_spend_in_usd,
        usd_per_local=settings.usd_per_local,
    )
    daily_final = _timed(
        "merge_meta",
        lambda: enrich_usd_columns(merge_daily_with_meta(daily_df, meta_for_merge), settings.usd_per_local),
    )
    if settings.daily_summary_usd_primary:
        daily_final = daily_summary_usd_primary(daily_final)
    bookkeeping_df = _timed(
        "bookkeeping",
        lambda: bookkeeping_us_monthly(shopify_orders, orders_df, daily_final),
    )
    missing_cost_df = build_missing_supplier_costs_report(orders_df)
    return PipelineArtifacts(
        orders_df=orders_df,
        order_df=order_df,
        daily_final=daily_final,
        meta_df=meta_df,
        meta_campaign_df=meta_campaign_df,
        bookkeeping_df=bookkeeping_df,
        missing_cost_df=missing_cost_df,
        shopify_orders=shopify_orders,
        meta_rows=meta_rows,
        shopify_updated_at_max=_extract_shopify_updated_at_max(shopify_orders),
    )


def _upload_core_tabs(settings: Settings, artifacts: PipelineArtifacts, *, started_total: float) -> None:
    def _run_step(step: str, fn, *, estimate: float = 0.0) -> None:
        _check_runtime_budget(started_total, step=step, estimated_seconds=estimate)
        _timed(step, fn)

    log_missing_supplier_costs(
        artifacts.orders_df,
        artifacts.missing_cost_df,
        sheet_tab=settings.missing_supplier_costs_tab,
    )
    if settings.missing_supplier_costs_tab:
        _run_step(
            "sheet_missing_supplier_costs",
            lambda: replace_worksheet_simple(settings, settings.missing_supplier_costs_tab, artifacts.missing_cost_df),
            estimate=3.0,
        )
        pause_between_sheet_uploads()
    _run_step(
        "sheet_orders_db",
        lambda: upload_dataframe(settings, artifacts.orders_df, SHEET_ORDERS_DB, layout_kind="orders"),
        estimate=20.0,
    )
    pause_between_sheet_uploads()
    _run_step(
        "sheet_order_level",
        lambda: upload_dataframe(settings, artifacts.order_df, SHEET_ORDER_LEVEL, layout_kind="order_level"),
        estimate=20.0,
    )
    pause_between_sheet_uploads()
    _run_step(
        "sheet_meta_data",
        lambda: upload_dataframe(settings, artifacts.meta_df, SHEET_META_DATA, layout_kind="meta"),
        estimate=10.0,
    )
    pause_between_sheet_uploads()
    _run_step(
        "sheet_daily_summary",
        lambda: upload_dataframe(settings, artifacts.daily_final, SHEET_DAILY, layout_kind="daily"),
        estimate=10.0,
    )


def _upload_tracking_tabs(settings: Settings, artifacts: PipelineArtifacts, *, started_total: float) -> None:
    def _run_step(step: str, fn, *, estimate: float = 0.0) -> None:
        _check_runtime_budget(started_total, step=step, estimated_seconds=estimate)
        _timed(step, fn)

    _run_step(
        "sheet_orders_db",
        lambda: upload_dataframe(settings, artifacts.orders_df, SHEET_ORDERS_DB, layout_kind="orders"),
        estimate=20.0,
    )
    pause_between_sheet_uploads()
    _run_step(
        "sheet_order_level",
        lambda: upload_dataframe(settings, artifacts.order_df, SHEET_ORDER_LEVEL, layout_kind="order_level"),
        estimate=20.0,
    )
    pause_between_sheet_uploads()
    _run_step(
        "sheet_daily_summary",
        lambda: upload_dataframe(settings, artifacts.daily_final, SHEET_DAILY, layout_kind="daily"),
        estimate=10.0,
    )


def _upload_reporting_tabs(settings: Settings, artifacts: PipelineArtifacts, *, started_total: float) -> None:
    def _run_step(step: str, fn, *, estimate: float = 0.0) -> None:
        _check_runtime_budget(started_total, step=step, estimated_seconds=estimate)
        _timed(step, fn)

    _run_step(
        "sheet_meta_data",
        lambda: upload_dataframe(settings, artifacts.meta_df, SHEET_META_DATA, layout_kind="meta"),
        estimate=10.0,
    )
    pause_between_sheet_uploads()
    if not artifacts.meta_campaign_df.empty or settings.meta_campaign_insights:
        _run_step(
            "sheet_meta_campaigns",
            lambda: upload_dataframe(
                settings,
                artifacts.meta_campaign_df,
                SHEET_META_CAMPAIGNS,
                layout_kind="meta_campaigns",
            ),
            estimate=10.0,
        )
        pause_between_sheet_uploads()
    _run_step(
        "sheet_bookkeeping",
        lambda: upload_dataframe(settings, artifacts.bookkeeping_df, SHEET_BOOKKEEPING, layout_kind="bookkeeping"),
        estimate=10.0,
    )


def _upload_full_tabs(settings: Settings, artifacts: PipelineArtifacts, *, started_total: float) -> None:
    def _run_step(step: str, fn, *, estimate: float = 0.0) -> None:
        _check_runtime_budget(started_total, step=step, estimated_seconds=estimate)
        _timed(step, fn)

    _upload_core_tabs(settings, artifacts, started_total=started_total)
    pause_between_sheet_uploads()
    if settings.meta_campaign_insights:
        _run_step(
            "sheet_meta_campaigns",
            lambda: upload_dataframe(
                settings,
                artifacts.meta_campaign_df,
                SHEET_META_CAMPAIGNS,
                layout_kind="meta_campaigns",
            ),
            estimate=10.0,
        )
        pause_between_sheet_uploads()
    _run_step(
        "sheet_bookkeeping",
        lambda: upload_dataframe(settings, artifacts.bookkeeping_df, SHEET_BOOKKEEPING, layout_kind="bookkeeping"),
        estimate=10.0,
    )


def _update_state_after_success(
    settings: Settings,
    state: PipelineState,
    *,
    mode: str,
    shopify_updated_at_max: str = "",
) -> None:
    now = utc_now_iso()
    next_state = state
    if shopify_updated_at_max:
        next_state = dc_replace(next_state, shopify_orders_updated_at_max=shopify_updated_at_max)
    next_state = dc_replace(
        next_state,
        meta_last_until_date=date.today().isoformat(),
        last_successful_run_kind=mode,
        last_error_summary="",
    )
    if mode == "core":
        next_state = dc_replace(next_state, last_core_sync_at=now)
    elif mode == "tracking":
        next_state = dc_replace(next_state, last_tracking_sync_at=now)
    elif mode == "reporting":
        next_state = dc_replace(next_state, last_reporting_sync_at=now)
    elif mode == "full":
        next_state = dc_replace(
            next_state,
            last_core_sync_at=now,
            last_tracking_sync_at=now,
            last_reporting_sync_at=now,
        )
    save_pipeline_state(settings, next_state)


def _save_state_error(settings: Settings, state: PipelineState, mode: str, exc: Exception) -> None:
    summary = f"[mode={mode}] {str(exc).replace(chr(10), ' ')[:1000]}"
    try:
        save_pipeline_state(settings, dc_replace(state, last_error_summary=summary))
    except Exception as save_exc:
        logger.warning(
            "Could not write PIPELINE_STATE after pipeline failure (quota or API); "
            "original error is still raised below: %s",
            save_exc,
        )


def _run_full(settings: Settings, cost_maps) -> PipelineArtifacts:
    log_shopify_auth_config(settings)
    max_workers = 3 if settings.meta_campaign_insights else 2
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        shopify_future = executor.submit(_fetch_shopify_full, settings)
        meta_future = executor.submit(_fetch_meta_daily_safe, settings)
        meta_campaign_future = (
            executor.submit(_fetch_meta_campaigns_safe, settings) if settings.meta_campaign_insights else None
        )
        shopify_orders, line_rows = shopify_future.result()
        meta_rows = meta_future.result()
        meta_campaign_rows = meta_campaign_future.result() if meta_campaign_future else None
    meta_rows = _merge_meta_rows_with_existing(settings, meta_rows)
    return _build_artifacts(
        settings,
        cost_maps,
        line_rows=line_rows,
        shopify_orders=shopify_orders,
        meta_rows=meta_rows,
        meta_campaign_rows=meta_campaign_rows,
    )


def _run_core(settings: Settings, cost_maps, state: PipelineState) -> PipelineArtifacts:
    existing_orders_df = _load_orders_db_df(settings)
    if existing_orders_df.empty or not state.shopify_orders_updated_at_max:
        logger.info("Incremental core bootstrap fallback -> full rebuild")
        return _run_full(settings, cost_maps)
    settings_core = _settings_for_core_mode(settings)
    log_shopify_auth_config(settings_core)
    updated_at_min = _updated_at_min_from_state(state, settings.pipeline_overlap_minutes)
    with ThreadPoolExecutor(max_workers=2) as executor:
        shopify_future = executor.submit(_fetch_shopify_incremental, settings_core, updated_at_min=updated_at_min)
        meta_future = executor.submit(_fetch_meta_daily_safe, settings)
        changed_orders, changed_rows = shopify_future.result()
        meta_rows = meta_future.result()
    meta_rows = _merge_meta_rows_with_existing(settings, meta_rows)
    merged_orders_df = _merge_orders_df(
        existing_orders_df,
        enrich_usd_columns(enrich_line_items(changed_rows, cost_maps), settings.usd_per_local),
    )
    line_rows = merged_orders_df.to_dict(orient="records")
    return _build_artifacts(
        settings,
        cost_maps,
        line_rows=line_rows,
        shopify_orders=changed_orders,
        meta_rows=meta_rows,
        meta_campaign_rows=None,
    )


def _run_tracking(settings: Settings, cost_maps, state: PipelineState) -> PipelineArtifacts:
    existing_orders_df = _load_orders_db_df(settings)
    if existing_orders_df.empty:
        logger.info("Tracking mode fallback -> full rebuild (missing ORDERS_DB)")
        return _run_full(settings, cost_maps)
    candidate_ids = _tracking_candidate_order_ids(settings, existing_orders_df)
    if not candidate_ids:
        logger.info("Tracking mode: no active shipments to refresh")
        meta_rows = _meta_frame_to_rows(_load_meta_df(settings), settings)
        line_rows = existing_orders_df.to_dict(orient="records")
        return _build_artifacts(
            settings,
            cost_maps,
            line_rows=line_rows,
            shopify_orders=[],
            meta_rows=meta_rows,
            meta_campaign_rows=None,
        )
    settings_tracking = _settings_for_tracking_mode(settings)
    log_shopify_auth_config(settings_tracking)
    with ThreadPoolExecutor(max_workers=2) as executor:
        orders_future = executor.submit(fetch_orders_by_ids, settings_tracking, candidate_ids)
        meta_future = executor.submit(_fetch_meta_daily_safe, settings)
        raw_orders = orders_future.result()
        meta_rows = meta_future.result()
    meta_rows = _merge_meta_rows_with_existing(settings, meta_rows)
    refreshed_orders, refreshed_rows = fetch_orders_and_line_rows(settings_tracking, orders=raw_orders)
    merged_orders_df = _merge_orders_df(
        existing_orders_df,
        enrich_usd_columns(enrich_line_items(refreshed_rows, cost_maps), settings.usd_per_local),
    )
    return _build_artifacts(
        settings,
        cost_maps,
        line_rows=merged_orders_df.to_dict(orient="records"),
        shopify_orders=refreshed_orders,
        meta_rows=meta_rows,
        meta_campaign_rows=None,
    )


def _run_reporting(settings: Settings, cost_maps, state: PipelineState) -> PipelineArtifacts:
    existing_orders_df = _load_orders_db_df(settings)
    if existing_orders_df.empty:
        logger.info("Reporting mode fallback -> full rebuild (missing ORDERS_DB)")
        return _run_full(settings, cost_maps)
    settings_reporting = _settings_for_reporting_mode(settings)
    with ThreadPoolExecutor(max_workers=3) as executor:
        shopify_future = executor.submit(_timed, "shopify_fetch", lambda: fetch_all_orders(settings_reporting))
        meta_future = executor.submit(_fetch_meta_daily_safe, settings)
        meta_campaign_future = executor.submit(_fetch_meta_campaigns_safe, settings)
        shopify_orders = shopify_future.result()
        meta_rows = meta_future.result()
        meta_campaign_rows = meta_campaign_future.result()
    meta_rows = _merge_meta_rows_with_existing(settings, meta_rows)
    return _build_artifacts(
        settings,
        cost_maps,
        line_rows=existing_orders_df.to_dict(orient="records"),
        shopify_orders=shopify_orders,
        meta_rows=meta_rows,
        meta_campaign_rows=meta_campaign_rows,
    )


def _log_completion(artifacts: PipelineArtifacts, started_total: float) -> None:
    logger.info(
        "Done. Line rows=%s, order rows=%s, meta days=%s, campaign rows=%s, daily rows=%s, bookkeeping months=%s",
        len(artifacts.orders_df),
        len(artifacts.order_df),
        len(artifacts.meta_df),
        len(artifacts.meta_campaign_df),
        len(artifacts.daily_final),
        len(artifacts.bookkeeping_df),
    )
    logger.info("timing_phase=total seconds=%.2f", time.perf_counter() - started_total)


def _sorted_df(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    cols = [c for c in keys if c in df.columns]
    if not cols:
        return df.reset_index(drop=True).fillna("")
    return df.sort_values(cols, kind="stable").reset_index(drop=True).fillna("")


def _ensure_frame_equal(name: str, actual: pd.DataFrame, expected: pd.DataFrame, keys: list[str]) -> None:
    left = _sorted_df(actual, keys)
    right = _sorted_df(expected, keys)
    if not left.equals(right):
        raise RuntimeError(f"Parity check failed for {name}: {len(left)} rows != {len(right)} rows or content differs")


def _run_parity_check(settings: Settings, cost_maps, state: PipelineState, mode: str, actual: PipelineArtifacts) -> None:
    logger.info("Running parity check for mode=%s …", mode)
    if mode == "core":
        baseline = _run_full(_settings_for_core_mode(settings), cost_maps)
        _ensure_frame_equal("ORDERS_DB", actual.orders_df, baseline.orders_df, ["Line_Item_ID"])
        _ensure_frame_equal("ORDER_LEVEL", actual.order_df, baseline.order_df, ["Order_ID"])
        _ensure_frame_equal("DAILY_SUMMARY", actual.daily_final, baseline.daily_final, ["Date"])
        _ensure_frame_equal("META_DATA", actual.meta_df, baseline.meta_df, ["Date"])
    elif mode == "tracking":
        baseline = _run_full(_settings_for_tracking_mode(settings), cost_maps)
        _ensure_frame_equal("ORDERS_DB", actual.orders_df, baseline.orders_df, ["Line_Item_ID"])
        _ensure_frame_equal("ORDER_LEVEL", actual.order_df, baseline.order_df, ["Order_ID"])
        _ensure_frame_equal("DAILY_SUMMARY", actual.daily_final, baseline.daily_final, ["Date"])
    elif mode == "reporting":
        baseline = _run_reporting(settings, cost_maps, state)
        _ensure_frame_equal("META_DATA", actual.meta_df, baseline.meta_df, ["Date"])
        _ensure_frame_equal("META_CAMPAIGNS", actual.meta_campaign_df, baseline.meta_campaign_df, ["Date", "Campaign_ID"])
        _ensure_frame_equal("BOOKKEEPING", actual.bookkeeping_df, baseline.bookkeeping_df, ["Month"])
    else:
        baseline = _run_full(settings, cost_maps)
        _ensure_frame_equal("ORDERS_DB", actual.orders_df, baseline.orders_df, ["Line_Item_ID"])
        _ensure_frame_equal("ORDER_LEVEL", actual.order_df, baseline.order_df, ["Order_ID"])
        _ensure_frame_equal("DAILY_SUMMARY", actual.daily_final, baseline.daily_final, ["Date"])
        _ensure_frame_equal("META_DATA", actual.meta_df, baseline.meta_df, ["Date"])
        _ensure_frame_equal("META_CAMPAIGNS", actual.meta_campaign_df, baseline.meta_campaign_df, ["Date", "Campaign_ID"])
        _ensure_frame_equal("BOOKKEEPING", actual.bookkeeping_df, baseline.bookkeeping_df, ["Month"])
    logger.info("Parity check passed for mode=%s", mode)


_RUN_OVERRIDE_BOOL_FIELDS = {
    "meta_campaign_insights",
    "meta_continue_on_error",
    "sheets_fancy_layout",
    "sheets_conditional_format",
    "shopify_fulfillment_enrich",
    "shopify_fulfillment_refetch_early",
    "shopify_graphql_fulfillment_verify",
}


def _apply_run_overrides(settings: Settings, overrides: dict[str, Any] | None) -> Settings:
    if not overrides:
        return settings
    changes: dict[str, Any] = {}
    for key in _RUN_OVERRIDE_BOOL_FIELDS:
        if key in overrides and isinstance(overrides[key], bool):
            changes[key] = overrides[key]
    if overrides.get("track17_enabled") is False:
        changes["track17_api_key"] = None
    if not changes:
        return settings
    logger.info(
        "pipeline_run_overrides=%s",
        ",".join(f"{key}={value}" for key, value in sorted(changes.items())),
    )
    return dc_replace(settings, **changes)


def main(mode_override: str | None = None, run_overrides: dict[str, Any] | None = None) -> int:
    try:
        settings = _apply_run_overrides(load_settings(), run_overrides)
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1

    started_total = time.perf_counter()
    state = load_pipeline_state(settings)
    mode = (mode_override or _normalize_mode(settings)).strip().lower()
    if mode not in {"auto", "full", "core", "tracking", "reporting"}:
        mode = "auto"
    if mode == "auto":
        mode = _choose_auto_mode(state)
        logger.info("pipeline_auto_selected_mode=%s", mode)
    phase = "supplier_costs"

    try:
        logger.info("Loading supplier costs from %s", settings.supplier_csv_path)
        cost_maps = _timed("supplier_costs", lambda: load_cost_maps(settings))
        logger.info(
            "pipeline_phase=%s_ok product_keys=%s sku_keys=%s sku_prefix_rules=%s order_single=%s learned=%s",
            phase,
            len(cost_maps.by_product),
            len(cost_maps.by_sku),
            len(cost_maps.sku_prefix_rules),
            len(cost_maps.by_order_single),
            len(cost_maps.learned_by_product_sku),
        )

        if mode == "full":
            phase = "full"
            artifacts = _run_full(settings, cost_maps)
            phase = "sheets"
            logger.info("Uploading to Google Sheets %r …", settings.google_sheet_id or settings.google_sheet_name)
            _timed("sheets", lambda: _upload_full_tabs(settings, artifacts, started_total=started_total))
        elif mode == "core":
            if not settings.pipeline_enable_incremental:
                logger.info("Core mode requested but PIPELINE_ENABLE_INCREMENTAL=0 -> fallback to full mode")
                phase = "full"
                artifacts = _run_full(settings, cost_maps)
                phase = "sheets"
                logger.info(
                    "Uploading to Google Sheets %r …",
                    settings.google_sheet_id or settings.google_sheet_name,
                )
                _timed("sheets", lambda: _upload_full_tabs(settings, artifacts, started_total=started_total))
                mode = "full"
            else:
                phase = "core"
                artifacts = _run_core(settings, cost_maps, state)
                phase = "sheets_core"
                logger.info("Uploading core tabs to Google Sheets %r …", settings.google_sheet_id or settings.google_sheet_name)
                _timed("sheets", lambda: _upload_core_tabs(settings, artifacts, started_total=started_total))
        elif mode == "tracking":
            phase = "tracking"
            artifacts = _run_tracking(settings, cost_maps, state)
            phase = "sheets_tracking"
            logger.info("Uploading tracking tabs to Google Sheets %r …", settings.google_sheet_id or settings.google_sheet_name)
            _timed("sheets", lambda: _upload_tracking_tabs(settings, artifacts, started_total=started_total))
        else:
            phase = "reporting"
            artifacts = _run_reporting(settings, cost_maps, state)
            phase = "sheets_reporting"
            logger.info("Uploading reporting tabs to Google Sheets %r …", settings.google_sheet_id or settings.google_sheet_name)
            _timed("sheets", lambda: _upload_reporting_tabs(settings, artifacts, started_total=started_total))

        if settings.pipeline_enable_parity_check:
            phase = "parity"
            _timed("parity", lambda: _run_parity_check(settings, cost_maps, state, mode, artifacts))

        _update_state_after_success(
            settings,
            state,
            mode=mode,
            shopify_updated_at_max=artifacts.shopify_updated_at_max,
        )
        _log_completion(artifacts, started_total)
        return 0
    except Exception as exc:
        _save_state_error(settings, state, mode, exc)
        if phase in {"full", "core"}:
            log_shopify_auth_config(settings)
        logger.exception("pipeline_failed phase=%s", phase)
        raise RuntimeError(f"[phase={phase}] {exc}") from exc


if __name__ == "__main__":
    sys.exit(main())
