"""Ecom Profit Engine — Shopify orders, Meta daily ad spend, supplier costs → Google Sheets."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace as dc_replace
from datetime import date, datetime, timezone
import logging
import os
import sys
import time
from typing import Any

import pandas as pd

from config import Settings, load_settings
from costs import load_cost_maps
from data_quality import build_missing_supplier_costs_report, log_missing_supplier_costs
from meta_ads import fetch_meta_daily_spend
from pipeline_state import PipelineState, load_pipeline_state, save_pipeline_state, utc_now_iso
from sheets import (
    pause_between_sheet_uploads,
    replace_worksheet_simple,
    try_read_worksheet_dataframe,
    upload_dataframe,
)
from shopify_auth import log_shopify_auth_config
from shopify_client import fetch_orders_and_line_rows
from transform import (
    daily_summary_from_orders,
    daily_summary_usd_primary,
    enrich_line_items,
    enrich_meta_usd_columns,
    enrich_usd_columns,
    merge_daily_with_meta,
    meta_rows_for_daily_merge,
    order_level_export_columns,
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
SHEET_DAILY = os.getenv("SHEET_TAB_DAILY_SUMMARY", "DAILY_SUMMARY").strip()
# Kept for env / docs compatibility; pipeline no longer writes these tabs.
SHEET_META_CAMPAIGNS = os.getenv("SHEET_TAB_META_CAMPAIGNS", "META_CAMPAIGNS").strip()
SHEET_BOOKKEEPING = os.getenv("SHEET_TAB_BOOKKEEPING", "BOOKKEEPING").strip()


@dataclass(frozen=True)
class PipelineArtifacts:
    orders_df: pd.DataFrame
    order_df: pd.DataFrame
    daily_final: pd.DataFrame
    meta_df: pd.DataFrame
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
            "Try SHEETS_FANCY_LAYOUT=0, SHEETS_CONDITIONAL_FORMAT=0, or increase function timeout."
        )


def _fetch_meta_daily_safe(settings: Settings) -> list[dict[str, Any]]:
    logger.info("Fetching Meta Ads spend …")
    try:
        return _timed("meta_fetch", lambda: fetch_meta_daily_spend(settings))
    except Exception as exc:
        if not settings.meta_continue_on_error:
            raise
        logger.warning(
            "Meta daily fetch failed, continuing with empty spend (META_CONTINUE_ON_ERROR=1): %s",
            exc,
        )
        return []


def _fetch_shopify_full(settings: Settings) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    logger.info("Fetching Shopify orders …")
    return _timed("shopify_fetch", lambda: fetch_orders_and_line_rows(settings))


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


def _extract_shopify_updated_at_max(orders: list[dict[str, Any]]) -> str:
    best: datetime | None = None
    for order in orders:
        dt = _parse_iso_dt(str(order.get("updated_at") or ""))
        if dt is not None and (best is None or dt > best):
            best = dt
    return best.isoformat() if best is not None else ""


def _drop_duplicate_columns(df: pd.DataFrame, *, label: str) -> pd.DataFrame:
    if df.empty or df.columns.is_unique:
        return df
    dupes = [str(c) for c in df.columns[df.columns.duplicated()].tolist()]
    logger.warning("%s: dropping duplicate columns from Sheet data: %s", label, dupes)
    return df.loc[:, ~df.columns.duplicated()].copy()


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


def _merge_meta_df(existing: pd.DataFrame, fresh: pd.DataFrame) -> pd.DataFrame:
    existing = _drop_duplicate_columns(existing, label="META_DATA existing")
    fresh = _drop_duplicate_columns(fresh, label="META_DATA fresh")
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
    else:
        return []
    src["Date"] = src["Date"].astype(str)
    src["Ad_Spend"] = pd.to_numeric(src["Ad_Spend"], errors="coerce").fillna(0.0).round(2)
    return src.to_dict(orient="records")


def _merge_meta_rows_with_existing(settings: Settings, fresh_meta_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    existing = _load_meta_df(settings)
    fresh = _build_meta_df(fresh_meta_rows, settings)
    merged = _merge_meta_df(existing, fresh)
    return _meta_frame_to_rows(merged, settings)


def _build_meta_df(meta_rows: list[dict[str, Any]], settings: Settings) -> pd.DataFrame:
    meta_df_all = enrich_meta_usd_columns(
        pd.DataFrame(meta_rows),
        usd_per_local=settings.usd_per_local,
        meta_spend_in_usd=settings.meta_spend_in_usd,
    )
    if "Ad_Spend_USD" in meta_df_all.columns:
        return meta_df_all[["Date", "Ad_Spend_USD"]].copy()
    return meta_df_all[["Date"]].copy() if "Date" in meta_df_all.columns else meta_df_all.copy()


def _meta_rows_from_campaign_rows(campaign_rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Daily Meta spend summed from campaign-level rows (tests / optional tooling)."""
    if not campaign_rows:
        return []
    df = pd.DataFrame(campaign_rows)
    if df.empty or "Date" not in df.columns or "Ad_Spend" not in df.columns:
        return []
    work = df[["Date", "Ad_Spend"]].copy()
    work["Date"] = work["Date"].astype(str).str.strip()
    work = work.loc[work["Date"] != ""].copy()
    work["Ad_Spend"] = pd.to_numeric(work["Ad_Spend"], errors="coerce").fillna(0.0)
    out = (
        work.groupby("Date", as_index=False)["Ad_Spend"]
        .sum()
        .sort_values("Date", kind="stable")
        .reset_index(drop=True)
    )
    out["Ad_Spend"] = out["Ad_Spend"].round(2)
    return out.to_dict(orient="records")


def _build_artifacts(
    settings: Settings,
    cost_maps,
    *,
    line_rows: list[dict[str, Any]],
    shopify_orders: list[dict[str, Any]],
    meta_rows: list[dict[str, Any]],
) -> PipelineArtifacts:
    orders_df = _timed(
        "enrich",
        lambda: enrich_usd_columns(enrich_line_items(line_rows, cost_maps), settings.usd_per_local),
    )
    order_df = _timed(
        "order_level",
        lambda: order_level_export_columns(
            enrich_usd_columns(order_level_summary(orders_df), settings.usd_per_local)
        ),
    )
    daily_df = _timed("daily", lambda: daily_summary_from_orders(orders_df))
    meta_df = _timed("meta_transform", lambda: _build_meta_df(meta_rows, settings))
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
    missing_cost_df = build_missing_supplier_costs_report(orders_df)
    return PipelineArtifacts(
        orders_df=orders_df,
        order_df=order_df,
        daily_final=daily_final,
        meta_df=meta_df,
        missing_cost_df=missing_cost_df,
        shopify_orders=shopify_orders,
        meta_rows=meta_rows,
        shopify_updated_at_max=_extract_shopify_updated_at_max(shopify_orders),
    )


def _upload_pipeline_tabs(settings: Settings, artifacts: PipelineArtifacts, *, started_total: float) -> None:
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


def _run_pipeline(settings: Settings, cost_maps) -> PipelineArtifacts:
    log_shopify_auth_config(settings)
    with ThreadPoolExecutor(max_workers=2) as executor:
        shopify_future = executor.submit(_fetch_shopify_full, settings)
        meta_future = executor.submit(_fetch_meta_daily_safe, settings)
        shopify_orders, line_rows = shopify_future.result()
        meta_rows = meta_future.result()
    meta_rows = _merge_meta_rows_with_existing(settings, meta_rows)
    return _build_artifacts(
        settings,
        cost_maps,
        line_rows=line_rows,
        shopify_orders=shopify_orders,
        meta_rows=meta_rows,
    )


def _update_state_after_success(
    settings: Settings,
    state: PipelineState,
    *,
    shopify_updated_at_max: str = "",
) -> None:
    now = utc_now_iso()
    next_state = state
    if shopify_updated_at_max:
        next_state = dc_replace(next_state, shopify_orders_updated_at_max=shopify_updated_at_max)
    next_state = dc_replace(
        next_state,
        meta_last_until_date=date.today().isoformat(),
        last_successful_run_kind="full",
        last_error_summary="",
        last_core_sync_at=now,
        last_tracking_sync_at=now,
        last_reporting_sync_at=now,
    )
    save_pipeline_state(settings, next_state)


def _save_state_error(settings: Settings, state: PipelineState, exc: Exception) -> None:
    summary = f"{str(exc).replace(chr(10), ' ')[:1000]}"
    try:
        save_pipeline_state(settings, dc_replace(state, last_error_summary=summary))
    except Exception as save_exc:
        logger.warning(
            "Could not write PIPELINE_STATE after pipeline failure (quota or API); "
            "original error is still raised below: %s",
            save_exc,
        )


def _log_completion(artifacts: PipelineArtifacts, started_total: float) -> None:
    logger.info(
        "Done. Line rows=%s, order rows=%s, meta days=%s, daily rows=%s",
        len(artifacts.orders_df),
        len(artifacts.order_df),
        len(artifacts.meta_df),
        len(artifacts.daily_final),
    )
    logger.info("timing_phase=total seconds=%.2f", time.perf_counter() - started_total)


_RUN_OVERRIDE_BOOL_FIELDS = frozenset(
    {
        "meta_continue_on_error",
        "sheets_fancy_layout",
        "sheets_conditional_format",
        "shopify_fulfillment_enrich",
        "shopify_fulfillment_refetch_early",
        "shopify_graphql_fulfillment_verify",
    }
)


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
    legacy_mode = (mode_override or "").strip().lower()
    if legacy_mode and legacy_mode not in {"", "full"}:
        logger.info(
            "pipeline_mode_deprecated=%r — only a single full run is supported; ignoring mode",
            legacy_mode,
        )

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

        phase = "pipeline"
        artifacts = _run_pipeline(settings, cost_maps)
        phase = "sheets"
        logger.info("Uploading to Google Sheets %r …", settings.google_sheet_id or settings.google_sheet_name)
        _timed("sheets", lambda: _upload_pipeline_tabs(settings, artifacts, started_total=started_total))

        _update_state_after_success(
            settings,
            state,
            shopify_updated_at_max=artifacts.shopify_updated_at_max,
        )
        _log_completion(artifacts, started_total)
        return 0
    except Exception as exc:
        _save_state_error(settings, state, exc)
        if phase in {"pipeline", "supplier_costs"}:
            log_shopify_auth_config(settings)
        logger.exception("pipeline_failed phase=%s", phase)
        raise RuntimeError(f"[phase={phase}] {exc}") from exc


if __name__ == "__main__":
    sys.exit(main())
