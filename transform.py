"""Join costs, compute profit, aggregate by order and by day."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from normalize import normalize_product_name


def enrich_line_items(rows: list[dict[str, Any]], cost_map: dict[str, float]) -> pd.DataFrame:
    """
    Unit cost from CSV is per unit; line COGS = unit_cost * Quantity.
    Missing cost maps to 0 for unit cost (PRD).
    """
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Date",
                "Order",
                "Order_ID",
                "Line_Item_ID",
                "Product",
                "Quantity",
                "Revenue",
                "Product_Cost",
                "Gross_Profit",
            ]
        )

    df["Product_Normalized"] = df["Product"].astype(str).map(normalize_product_name)
    unit_cost = df["Product_Normalized"].map(cost_map).fillna(0).astype(float)
    qty = df["Quantity"].astype(float) if "Quantity" in df.columns else pd.Series(1.0, index=df.index)
    df["Product_Cost"] = (unit_cost * qty).round(2)
    df["Gross_Profit"] = (df["Revenue"].astype(float) - df["Product_Cost"]).round(2)
    df = df.drop(columns=["Product_Normalized"], errors="ignore")
    return df


def order_level_summary(df: pd.DataFrame) -> pd.DataFrame:
    """One row per order."""
    if df.empty:
        return pd.DataFrame(
            columns=["Date", "Order", "Order_ID", "Revenue", "Product_Cost", "Gross_Profit"]
        )
    grouped = (
        df.groupby(["Date", "Order", "Order_ID"], dropna=False)
        .agg(
            Revenue=("Revenue", "sum"),
            Product_Cost=("Product_Cost", "sum"),
            Gross_Profit=("Gross_Profit", "sum"),
        )
        .reset_index()
    )
    grouped["Revenue"] = grouped["Revenue"].round(2)
    grouped["Product_Cost"] = grouped["Product_Cost"].round(2)
    grouped["Gross_Profit"] = grouped["Gross_Profit"].round(2)
    return grouped


def daily_summary_from_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Daily totals from line-level data."""
    if df.empty:
        return pd.DataFrame(columns=["Date", "Revenue", "Product_Cost", "Gross_Profit"])
    daily = (
        df.groupby("Date", dropna=False)
        .agg(
            Revenue=("Revenue", "sum"),
            Product_Cost=("Product_Cost", "sum"),
            Gross_Profit=("Gross_Profit", "sum"),
        )
        .reset_index()
    )
    daily["Revenue"] = daily["Revenue"].round(2)
    daily["Product_Cost"] = daily["Product_Cost"].round(2)
    daily["Gross_Profit"] = daily["Gross_Profit"].round(2)
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
