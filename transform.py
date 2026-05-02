"""Join costs, compute profit, aggregate by order and by day."""

from __future__ import annotations

import logging
import re
from typing import Any

import numpy as np
import pandas as pd

from costs import CostMaps, build_product_lineage_index
from normalize import (
    normalize_order_number,
    normalize_product_name,
    normalize_sku,
    product_title_family_levels,
)

logger = logging.getLogger(__name__)
_DELIVERED_WORD_RE = re.compile(r"\bdelivered\b", re.IGNORECASE)


def _is_delivered_row(row: pd.Series) -> bool:
    """Delivery detection for summary counts (Shopify + carrier fallback)."""
    delivery_status = str(row.get("Delivery_Status") or "").strip().lower()
    if delivery_status == "delivered":
        return True
    shipment_status = str(row.get("Shipment_Status") or "").strip().lower()
    if shipment_status == "delivered":
        return True
    carrier_status = str(row.get("Carrier_Tracking_Status") or "").strip()
    return bool(_DELIVERED_WORD_RE.search(carrier_status))


def _line_unit_cost(
    row: pd.Series,
    cost_maps: CostMaps,
    *,
    order_counts: dict[str, int],
    lineage_map: dict[str, float],
) -> float:
    k = row["Product_Normalized"]
    for cand in product_title_family_levels(k):
        pl = lineage_map.get(cand)
        if pl is not None and pl > 0:
            return float(pl)
    sku_raw = str(row.get("SKU") or "").strip()
    sk = normalize_sku(sku_raw)
    if sk:
        uc = cost_maps.by_sku.get(sk)
        if uc is not None:
            return float(uc)
        for prefix, cost in cost_maps.sku_prefix_rules:
            if sk.startswith(prefix):
                return float(cost)
    ord_name = str(row.get("Order") or "").strip()
    if ord_name and order_counts.get(ord_name, 0) == 1:
        on = normalize_order_number(ord_name)
        if on:
            uo = cost_maps.by_order_single.get(on)
            if uo is not None and uo > 0:
                return float(uo)
    lc = cost_maps.learned_by_product_sku
    if (k, sk) in lc:
        return float(lc[(k, sk)])
    if (k, "") in lc:
        return float(lc[(k, "")])
    return 0.0


def enrich_line_items(rows: list[dict[str, Any]], cost_maps: CostMaps) -> pd.DataFrame:
    """
    Unit cost resolution order: product title (vrátane „rovnaký model, iná farba“ cez odseknuté
    koncovky `` - …``) → presné SKU → ITEM_CATALOG prefix → jednopoložková objednávka → learned ORDERS_DB.
    """
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
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
                "Refunds_Total",
                "Refund_Base_Amount",
                "Refund_Ratio_pct",
                "Refund_Bucket",
                "Product_Cost",
                "Gross_Profit",
                "Payment_Gateway_Names",
                "Payment_Net",
                "Payment_Net_Estimate",
            ]
        )

    if "SKU" not in df.columns:
        df["SKU"] = ""
    df["Product_Normalized"] = df["Product"].astype(str).map(normalize_product_name)
    order_counts = df.groupby(df["Order"].astype(str), sort=False).size().to_dict()
    lineage_map = cost_maps.by_product_lineage
    if not lineage_map and cost_maps.by_product:
        lineage_map = build_product_lineage_index(cost_maps.by_product)
    unit_cost = df.apply(
        lambda r: _line_unit_cost(
            r,
            cost_maps,
            order_counts=order_counts,
            lineage_map=lineage_map,
        ),
        axis=1,
    ).astype(float)
    qty = df["Quantity"].astype(float) if "Quantity" in df.columns else pd.Series(1.0, index=df.index)
    df["Product_Cost"] = (unit_cost * qty).round(2)
    df["Gross_Profit"] = (df["Revenue"].astype(float) - df["Product_Cost"]).round(2)
    if "Payment_Gateway_Names" not in df.columns:
        df["Payment_Gateway_Names"] = ""
    else:
        df["Payment_Gateway_Names"] = df["Payment_Gateway_Names"].fillna("").astype(str)
    df = df.drop(columns=["Product_Normalized"], errors="ignore")
    return df


def order_level_summary(df: pd.DataFrame) -> pd.DataFrame:
    """One row per order."""
    if df.empty:
        return pd.DataFrame(
            columns=[
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
                "Revenue",
                "Refunds_Total",
                "Refund_Base_Amount",
                "Refund_Ratio_pct",
                "Refund_Bucket",
                "Net_Revenue_After_Refunds",
                "Product_Cost",
                "Gross_Profit",
                "Gross_Profit_After_Refunds",
                "Payment_Gateway_Names",
                "Payment_Net",
                "Payment_Net_Estimate",
            ]
        )
    agg: dict[str, Any] = {
        "Revenue": ("Revenue", "sum"),
        "Product_Cost": ("Product_Cost", "sum"),
        "Gross_Profit": ("Gross_Profit", "sum"),
    }
    if "Refunds_Total" in df.columns:
        agg["Refunds_Total"] = ("Refunds_Total", "max")
    if "Refund_Base_Amount" in df.columns:
        agg["Refund_Base_Amount"] = ("Refund_Base_Amount", "max")
    if "Refund_Ratio_pct" in df.columns:
        agg["Refund_Ratio_pct"] = ("Refund_Ratio_pct", "max")
    if "Refund_Bucket" in df.columns:
        agg["Refund_Bucket"] = ("Refund_Bucket", "first")
    if "Payment_Net" in df.columns:
        agg["Payment_Net"] = ("Payment_Net", "max")
    if "Payment_Gateway_Names" in df.columns:
        agg["Payment_Gateway_Names"] = ("Payment_Gateway_Names", "first")
    if "Payment_Net_Estimate" in df.columns:
        agg["Payment_Net_Estimate"] = ("Payment_Net_Estimate", "max")
    for col in (
        "Fulfillment_Status",
        "Shipment_Status",
        "Delivery_Status",
        "Tracking_Numbers",
        "Tracking_Companies",
        "Carrier_Tracking_Status",
        "Shipped_Date",
        "Days_In_Transit",
        "Refund_Bucket",
    ):
        if col in df.columns:
            agg[col] = (col, "first")
    grouped = df.groupby(["Date", "Order", "Order_ID"], dropna=False).agg(**agg).reset_index()
    refunds_num = pd.to_numeric(
        grouped["Refunds_Total"] if "Refunds_Total" in grouped.columns else pd.Series(0.0, index=grouped.index),
        errors="coerce",
    ).fillna(0.0)
    revenue_num = pd.to_numeric(grouped["Revenue"], errors="coerce").fillna(0.0)
    cost_num = pd.to_numeric(grouped["Product_Cost"], errors="coerce").fillna(0.0)
    grouped["Net_Revenue_After_Refunds"] = (revenue_num - refunds_num).round(2)
    grouped["Gross_Profit_After_Refunds"] = (grouped["Net_Revenue_After_Refunds"] - cost_num).round(2)
    if "Refund_Ratio_pct" in grouped.columns:
        grouped["Refund_Ratio_pct"] = pd.to_numeric(grouped["Refund_Ratio_pct"], errors="coerce").round(2)
    if "Refunds_Total" in grouped.columns:
        grouped["Refunds_Total"] = refunds_num.round(2)
    if "Refund_Base_Amount" in grouped.columns:
        grouped["Refund_Base_Amount"] = (
            pd.to_numeric(grouped["Refund_Base_Amount"], errors="coerce").fillna(0.0).round(2)
        )
    preferred = [
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
        "Revenue",
        "Refunds_Total",
        "Refund_Base_Amount",
        "Refund_Ratio_pct",
        "Refund_Bucket",
        "Net_Revenue_After_Refunds",
        "Payment_Net",
        "Payment_Gateway_Names",
        "Payment_Net_Estimate",
        "Product_Cost",
        "Gross_Profit",
        "Gross_Profit_After_Refunds",
    ]
    ordered = [c for c in preferred if c in grouped.columns]
    rest = [c for c in grouped.columns if c not in ordered]
    grouped = grouped[ordered + rest]
    grouped["Revenue"] = grouped["Revenue"].round(2)
    grouped["Product_Cost"] = grouped["Product_Cost"].round(2)
    grouped["Gross_Profit"] = grouped["Gross_Profit"].round(2)
    return grouped


def bookkeeping_monthly_from_daily(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Monthly management P&L rollup for a **BOOKKEEPING** sheet (not statutory accounts).

    Input is the same frame as **DAILY_SUMMARY** after Meta merge (and optional
    ``daily_summary_usd_primary``): at least ``Date``, ``Revenue``, ``Product_Cost``,
    ``Gross_Profit``, optionally ``Ad_Spend``.
    """
    empty_cols = [
        "Month",
        "Sales_Revenue",
        "COGS",
        "Gross_Profit",
        "Marketing_Spend",
        "Net_Profit",
    ]
    if daily_df.empty:
        return pd.DataFrame(columns=empty_cols)
    if "Date" not in daily_df.columns:
        return pd.DataFrame(columns=empty_cols)

    d = daily_df.copy()
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
    d = d.dropna(subset=["Date"])
    if d.empty:
        return pd.DataFrame(columns=empty_cols)

    d["Month"] = d["Date"].dt.to_period("M").astype(str)

    agg: dict[str, Any] = {}
    for col in ("Revenue", "Product_Cost", "Gross_Profit"):
        if col in d.columns:
            agg[col] = "sum"
    if "Ad_Spend" in d.columns:
        agg["Ad_Spend"] = "sum"

    if not agg:
        return pd.DataFrame(columns=empty_cols)

    g = d.groupby("Month", as_index=False).agg(agg)
    g["Sales_Revenue"] = pd.to_numeric(g["Revenue"], errors="coerce").fillna(0.0).round(2)
    g["COGS"] = pd.to_numeric(g["Product_Cost"], errors="coerce").fillna(0.0).round(2)
    g["Gross_Profit"] = pd.to_numeric(g["Gross_Profit"], errors="coerce").fillna(0.0).round(2)
    if "Ad_Spend" in g.columns:
        g["Marketing_Spend"] = pd.to_numeric(g["Ad_Spend"], errors="coerce").fillna(0.0).round(2)
    else:
        g["Marketing_Spend"] = 0.0
    g["Net_Profit"] = (g["Gross_Profit"] - g["Marketing_Spend"]).round(2)

    out = g[empty_cols].copy()
    return out.sort_values("Month").reset_index(drop=True)


def daily_summary_from_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Daily totals from line-level data."""
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Date",
                "Revenue",
                "Product_Cost",
                "Gross_Profit",
                "Orders_Total",
                "Orders_Delivered",
                "Orders_Undelivered",
            ]
        )
    daily = (
        df.groupby("Date", dropna=False)
        .agg(
            Revenue=("Revenue", "sum"),
            Product_Cost=("Product_Cost", "sum"),
            Gross_Profit=("Gross_Profit", "sum"),
        )
        .reset_index()
    )
    if "Order" in df.columns and "Delivery_Status" in df.columns:
        keep_cols = ["Date", "Order", "Delivery_Status"]
        for optional in ("Shipment_Status", "Carrier_Tracking_Status"):
            if optional in df.columns:
                keep_cols.append(optional)
        order_delivery = (
            df[keep_cols]
            .dropna(subset=["Order"])
            .drop_duplicates(subset=["Date", "Order"])
            .copy()
        )
        order_delivery["Is_Delivered"] = order_delivery.apply(_is_delivered_row, axis=1)
        counts = (
            order_delivery.groupby("Date", dropna=False)
            .agg(
                Orders_Total=("Order", "nunique"),
                Orders_Delivered=("Is_Delivered", "sum"),
            )
            .reset_index()
        )
        counts["Orders_Undelivered"] = (
            counts["Orders_Total"] - counts["Orders_Delivered"]
        )
        daily = daily.merge(counts, on="Date", how="left")
    else:
        daily["Orders_Total"] = 0
        daily["Orders_Delivered"] = 0
        daily["Orders_Undelivered"] = 0

    daily["Revenue"] = daily["Revenue"].round(2)
    daily["Product_Cost"] = daily["Product_Cost"].round(2)
    daily["Gross_Profit"] = daily["Gross_Profit"].round(2)
    for col in ("Orders_Total", "Orders_Delivered", "Orders_Undelivered"):
        if col in daily.columns:
            daily[col] = pd.to_numeric(daily[col], errors="coerce").fillna(0).astype(int)
    return daily


def merge_daily_with_meta(daily: pd.DataFrame, meta_rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Combine daily P&L with Meta spend; Marketing_ROAS = Revenue / Ad_Spend when spend > 0."""
    meta = pd.DataFrame(meta_rows)
    if not meta.empty:
        meta = meta.groupby("Date", as_index=False)["Ad_Spend"].sum()
    if meta.empty:
        out = daily.copy()
        out["Ad_Spend"] = np.nan
        out["Marketing_ROAS"] = np.nan
        return out

    merged = daily.merge(meta, on="Date", how="outer").sort_values("Date")
    merged["Revenue"] = merged["Revenue"].fillna(0)
    merged["Product_Cost"] = merged["Product_Cost"].fillna(0)
    merged["Gross_Profit"] = merged["Gross_Profit"].fillna(0)

    def roas(row: pd.Series) -> float | None:
        spend = row.get("Ad_Spend")
        rev = float(row.get("Revenue") or 0)
        if spend is None or (isinstance(spend, float) and np.isnan(spend)):
            return None
        sp = float(spend)
        if sp <= 0:
            return None
        return round(rev / sp, 4)

    merged["Marketing_ROAS"] = merged.apply(roas, axis=1)
    return merged


def enrich_usd_columns(df: pd.DataFrame, usd_per_local: float | None) -> pd.DataFrame:
    """
    Append *_USD columns by multiplying local currency columns by USD_PER_LOCAL_UNIT
    (how many USD for one unit of shop/report currency, e.g. 0.65 if 1 AUD ≈ 0.65 USD).
    """
    if not usd_per_local or usd_per_local <= 0:
        return df
    out = df.copy()
    pairs = [
        ("Revenue", "Revenue_USD"),
        ("Product_Cost", "Product_Cost_USD"),
        ("Gross_Profit", "Gross_Profit_USD"),
        ("Ad_Spend", "Ad_Spend_USD"),
        ("Purchase_Value", "Purchase_Value_USD"),
    ]
    for src, dst in pairs:
        if src not in out.columns:
            continue
        s = pd.to_numeric(out[src], errors="coerce").fillna(0.0)
        out[dst] = (s * float(usd_per_local)).round(2)
    return out


def daily_summary_usd_primary(df: pd.DataFrame) -> pd.DataFrame:
    """
    For DAILY_SUMMARY only: drop shop-currency columns and use ``*_USD`` as the main
    columns (renamed to Revenue, Product_Cost, Gross_Profit, Ad_Spend). Recomputes
    ``Marketing_ROAS`` and adds ``Net_Profit`` = Gross_Profit − Ad_Spend (same basis as ROAS).

    No-op when ``USD_PER_LOCAL_UNIT`` was not applied (missing ``*_USD`` columns).
    """
    required = (
        "Revenue_USD",
        "Product_Cost_USD",
        "Gross_Profit_USD",
        "Ad_Spend_USD",
    )
    if not all(c in df.columns for c in required):
        logger.info(
            "daily_summary_usd_primary skipped (set USD_PER_LOCAL_UNIT for %s)",
            ", ".join(required),
        )
        return df
    out = df.copy()
    for c in ("Revenue", "Product_Cost", "Gross_Profit", "Ad_Spend"):
        out = out.drop(columns=[c], errors="ignore")
    out = out.rename(
        columns={
            "Revenue_USD": "Revenue",
            "Product_Cost_USD": "Product_Cost",
            "Gross_Profit_USD": "Gross_Profit",
            "Ad_Spend_USD": "Ad_Spend",
        }
    )

    def roas(row: pd.Series) -> float | None:
        spend = row.get("Ad_Spend")
        rev = float(row.get("Revenue") or 0)
        if spend is None or (isinstance(spend, float) and np.isnan(spend)):
            return None
        sp = float(spend)
        if sp <= 0:
            return None
        return round(rev / sp, 4)

    out["Marketing_ROAS"] = out.apply(roas, axis=1)
    gross = pd.to_numeric(out["Gross_Profit"], errors="coerce").fillna(0.0)
    spend_num = pd.to_numeric(out["Ad_Spend"], errors="coerce").fillna(0.0)
    out["Net_Profit"] = (gross - spend_num).round(2)
    preferred = [
        "Date",
        "Revenue",
        "Product_Cost",
        "Gross_Profit",
        "Ad_Spend",
        "Marketing_ROAS",
        "Net_Profit",
    ]
    rest = [c for c in out.columns if c not in preferred]
    out = out[[c for c in preferred if c in out.columns] + rest]
    return out


def enrich_meta_usd_columns(
    df: pd.DataFrame,
    *,
    usd_per_local: float | None,
    meta_spend_in_usd: bool,
) -> pd.DataFrame:
    """
    Meta Marketing API returns spend (and usually purchase value) in the **ad account
    currency** — often USD, not the same as Shopify store currency (e.g. AUD).

    When ``meta_spend_in_usd`` is True, treat ``Ad_Spend`` / ``Purchase_Value`` as
    already USD: ``*_USD`` columns are copies (no multiply by ``USD_PER_LOCAL_UNIT``).

    When False, treat those amounts like shop/report currency (same behaviour as
    :func:`enrich_usd_columns` for Meta columns only).
    """
    if df.empty:
        return df
    if meta_spend_in_usd:
        out = df.copy()
        if "Ad_Spend" in out.columns:
            s = pd.to_numeric(out["Ad_Spend"], errors="coerce").fillna(0.0).round(2)
            out["Ad_Spend_USD"] = s
        if "Purchase_Value" in out.columns:
            s = pd.to_numeric(out["Purchase_Value"], errors="coerce").fillna(0.0).round(2)
            out["Purchase_Value_USD"] = s
        return out
    return enrich_usd_columns(df, usd_per_local)


def meta_rows_for_daily_merge(
    meta_rows: list[dict[str, Any]],
    *,
    meta_spend_in_usd: bool,
    usd_per_local: float | None,
) -> list[dict[str, Any]]:
    """
    Daily P&L merge uses ``Ad_Spend`` in the **same currency as Revenue** (typically
    shop currency). If Meta reports USD and ``usd_per_local`` is set (USD per 1 AUD),
    convert each day's Meta spend from USD to AUD: ``aud = usd / usd_per_local``.
    """
    if not meta_rows:
        return meta_rows
    if not meta_spend_in_usd or not usd_per_local or usd_per_local <= 0:
        return list(meta_rows)
    out: list[dict[str, Any]] = []
    for r in meta_rows:
        usd = float(r.get("Ad_Spend") or 0)
        aud = usd / float(usd_per_local)
        out.append({**r, "Ad_Spend": round(aud, 2)})
    return out
