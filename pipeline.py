"""Ecom Profit Engine — fetch Shopify + Meta + CSV costs, write Google Sheets."""

from __future__ import annotations

import logging
import os
import sys

import pandas as pd

from config import load_settings
from costs import load_cost_maps
from data_quality import build_missing_supplier_costs_report, log_missing_supplier_costs
from meta_ads import fetch_meta_campaign_insights, fetch_meta_daily_spend
from bookkeeping_us import bookkeeping_us_monthly
from shopify_client import fetch_orders_and_line_rows
from shopify_auth import log_shopify_auth_config
from sheets import pause_between_sheet_uploads, replace_worksheet_simple, upload_dataframe
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

# Tab names (override with env if needed)
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


def main() -> int:
    try:
        settings = load_settings()
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1

    phase = "supplier_costs"
    try:
        logger.info("Loading supplier costs from %s", settings.supplier_csv_path)
        cost_maps = load_cost_maps(settings)
        logger.info(
            "pipeline_phase=%s_ok product_keys=%s sku_keys=%s sku_prefix_rules=%s "
            "order_single=%s learned=%s",
            phase,
            len(cost_maps.by_product),
            len(cost_maps.by_sku),
            len(cost_maps.sku_prefix_rules),
            len(cost_maps.by_order_single),
            len(cost_maps.learned_by_product_sku),
        )

        phase = "shopify"
        log_shopify_auth_config(settings)
        logger.info("Fetching Shopify orders …")
        shopify_orders, line_rows = fetch_orders_and_line_rows(settings)
        logger.info("pipeline_phase=%s_ok line_rows=%s", phase, len(line_rows))

        phase = "enrich"
        logger.info("Enriching line items with costs …")
        orders_df = enrich_usd_columns(
            enrich_line_items(line_rows, cost_maps),
            settings.usd_per_local,
        )

        phase = "order_level"
        logger.info("Building order-level summary …")
        order_df = enrich_usd_columns(
            order_level_summary(orders_df),
            settings.usd_per_local,
        )

        phase = "daily"
        logger.info("Building daily summary …")
        daily_df = daily_summary_from_orders(orders_df)

        phase = "meta"
        logger.info("Fetching Meta Ads spend …")
        meta_rows = fetch_meta_daily_spend(settings)
        if settings.meta_spend_in_usd and not settings.usd_per_local:
            logger.warning(
                "META_SPEND_IN_USD=1 but USD_PER_LOCAL_UNIT is unset: "
                "daily merge may mix Meta USD with Shopify shop currency for ROAS."
            )
        meta_df_all = enrich_meta_usd_columns(
            pd.DataFrame(meta_rows),
            usd_per_local=settings.usd_per_local,
            meta_spend_in_usd=settings.meta_spend_in_usd,
        )
        # META_DATA is intentionally USD-only.
        if "Ad_Spend_USD" in meta_df_all.columns:
            meta_df = meta_df_all[["Date", "Ad_Spend_USD"]].copy()
        else:
            meta_df = meta_df_all[["Date"]].copy() if "Date" in meta_df_all.columns else meta_df_all.copy()

        meta_campaign_df = pd.DataFrame()
        if settings.meta_campaign_insights:
            phase = "meta_campaigns"
            logger.info("Fetching Meta campaign insights (spend + conversions) …")
            _mc_raw = fetch_meta_campaign_insights(settings)
            meta_campaign_df = enrich_meta_usd_columns(
                pd.DataFrame(_mc_raw) if _mc_raw else pd.DataFrame(columns=_META_CAMPAIGN_COLUMNS),
                usd_per_local=settings.usd_per_local,
                meta_spend_in_usd=settings.meta_spend_in_usd,
            )

        phase = "merge_meta"
        logger.info("Merging daily summary with Meta spend …")
        meta_for_merge = meta_rows_for_daily_merge(
            meta_rows,
            meta_spend_in_usd=settings.meta_spend_in_usd,
            usd_per_local=settings.usd_per_local,
        )
        daily_final = enrich_usd_columns(
            merge_daily_with_meta(daily_df, meta_for_merge),
            settings.usd_per_local,
        )
        if settings.daily_summary_usd_primary:
            daily_final = daily_summary_usd_primary(daily_final)

        bookkeeping_df = bookkeeping_us_monthly(shopify_orders, orders_df, daily_final)

        phase = "sheets"
        _sheet_target = settings.google_sheet_id or settings.google_sheet_name
        logger.info("Uploading to Google Sheets %r …", _sheet_target)
        missing_cost_df = build_missing_supplier_costs_report(orders_df)
        log_missing_supplier_costs(
            orders_df,
            missing_cost_df,
            sheet_tab=settings.missing_supplier_costs_tab,
        )
        if settings.missing_supplier_costs_tab:
            replace_worksheet_simple(
                settings,
                settings.missing_supplier_costs_tab,
                missing_cost_df,
            )
            pause_between_sheet_uploads()
        upload_dataframe(settings, orders_df, SHEET_ORDERS_DB, layout_kind="orders")
        pause_between_sheet_uploads()
        upload_dataframe(settings, order_df, SHEET_ORDER_LEVEL, layout_kind="order_level")
        pause_between_sheet_uploads()
        upload_dataframe(settings, meta_df, SHEET_META_DATA, layout_kind="meta")
        pause_between_sheet_uploads()
        if settings.meta_campaign_insights:
            upload_dataframe(
                settings,
                meta_campaign_df,
                SHEET_META_CAMPAIGNS,
                layout_kind="meta_campaigns",
            )
            pause_between_sheet_uploads()
        upload_dataframe(settings, daily_final, SHEET_DAILY, layout_kind="daily")
        pause_between_sheet_uploads()
        upload_dataframe(
            settings,
            bookkeeping_df,
            SHEET_BOOKKEEPING,
            layout_kind="bookkeeping",
        )

        logger.info(
            "Done. Line rows=%s, order rows=%s, meta days=%s, campaign rows=%s, daily rows=%s, bookkeeping months=%s",
            len(orders_df),
            len(order_df),
            len(meta_df),
            len(meta_campaign_df),
            len(daily_final),
            len(bookkeeping_df),
        )
        return 0
    except Exception as exc:
        if phase == "shopify":
            log_shopify_auth_config(settings)
        logger.exception("pipeline_failed phase=%s", phase)
        raise RuntimeError(f"[phase={phase}] {exc}") from exc


if __name__ == "__main__":
    sys.exit(main())
