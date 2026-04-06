"""Ecom Profit Engine — fetch Shopify + Meta + CSV costs, write Google Sheets."""

from __future__ import annotations

import logging
import os
import sys

import pandas as pd

from config import load_settings
from costs import load_cost_map
from meta_ads import fetch_meta_daily_spend
from shopify_client import fetch_order_line_rows
from sheets import upload_dataframe
from transform import (
    daily_summary_from_orders,
    enrich_line_items,
    merge_daily_with_meta,
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
SHEET_DAILY = os.getenv("SHEET_TAB_DAILY_SUMMARY", "DAILY_SUMMARY").strip()


def main() -> int:
    try:
        settings = load_settings()
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1

    logger.info("Loading supplier costs from %s", settings.supplier_csv_path)
    cost_map = load_cost_map(settings.supplier_csv_path)

    logger.info("Fetching Shopify orders …")
    line_rows = fetch_order_line_rows(settings)

    logger.info("Enriching line items with costs …")
    orders_df = enrich_line_items(line_rows, cost_map)

    logger.info("Building order-level summary …")
    order_df = order_level_summary(orders_df)

    logger.info("Building daily summary …")
    daily_df = daily_summary_from_orders(orders_df)

    logger.info("Fetching Meta Ads spend …")
    meta_rows = fetch_meta_daily_spend(settings)
    meta_df = pd.DataFrame(meta_rows)

    logger.info("Merging daily summary with Meta spend …")
    daily_final = merge_daily_with_meta(daily_df, meta_rows)

    logger.info("Uploading to Google Sheets %r …", settings.google_sheet_name)
    upload_dataframe(settings, orders_df, SHEET_ORDERS_DB)
    upload_dataframe(settings, order_df, SHEET_ORDER_LEVEL)
    upload_dataframe(settings, meta_df, SHEET_META_DATA)
    upload_dataframe(settings, daily_final, SHEET_DAILY)

    logger.info(
        "Done. Line rows=%s, order rows=%s, meta days=%s, daily rows=%s",
        len(orders_df),
        len(order_df),
        len(meta_df),
        len(daily_final),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
