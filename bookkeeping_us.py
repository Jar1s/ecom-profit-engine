"""
US-oriented monthly bookkeeping rollup (management P&L, not GAAP statements).

Uses Shopify Admin order fields (shop currency): gross line items, discounts, subtotal,
shipping, sales tax collected, refunds; COGS from enriched line items; Meta ad spend
from the merged daily series.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

_US_COLS = [
    "Month",
    "Gross_merchandise",
    "Order_discounts",
    "Product_sales_net",
    "Shipping_revenue",
    "Sales_tax_collected",
    "Refunds_total",
    "Net_sales",
    "COGS",
    "Gross_profit",
    "Marketing_advertising",
    "Operating_income",
]


def _f(val: Any) -> float:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _shipping_shop_amount(order: dict[str, Any]) -> float:
    tsp = order.get("total_shipping_price_set")
    if not isinstance(tsp, dict):
        return 0.0
    sm = tsp.get("shop_money")
    if not isinstance(sm, dict):
        return 0.0
    return _f(sm.get("amount"))


def _refunds_total(order: dict[str, Any]) -> float:
    """Sum refund transaction amounts (absolute shop currency)."""
    total = 0.0
    for ref in order.get("refunds") or []:
        if not isinstance(ref, dict):
            continue
        for t in ref.get("transactions") or []:
            if not isinstance(t, dict):
                continue
            total += abs(_f(t.get("amount")))
    return round(total, 2)


def _order_us_row(order: dict[str, Any]) -> dict[str, Any] | None:
    created = order.get("created_at") or ""
    date_str = created[:10] if len(created) >= 10 else ""
    if not date_str:
        return None
    oid = order.get("id")
    gross = _f(order.get("total_line_items_price"))
    disc = _f(order.get("total_discounts"))
    sub = _f(order.get("subtotal_price"))
    tax = _f(order.get("total_tax"))
    ship = _shipping_shop_amount(order)
    ref = _refunds_total(order)
    net_sales = round(sub + ship - ref, 2)
    return {
        "Date": date_str,
        "Order_ID": oid,
        "Gross_merchandise": round(gross, 2),
        "Order_discounts": round(disc, 2),
        "Product_sales_net": round(sub, 2),
        "Shipping_revenue": round(ship, 2),
        "Sales_tax_collected": round(tax, 2),
        "Refunds_total": ref,
        "Net_sales": net_sales,
    }


def bookkeeping_us_monthly(
    orders: list[dict[str, Any]],
    orders_df: pd.DataFrame,
    daily_final: pd.DataFrame,
) -> pd.DataFrame:
    """
    One row per calendar month with US-style column names.

    ``orders``: raw Shopify order dicts from Admin REST.
    ``orders_df``: enriched line items (Date, Order_ID, Product_Cost, …).
    ``daily_final``: DAILY_SUMMARY after Meta merge (Date, Ad_Spend, …).
    """
    if not orders:
        return pd.DataFrame(columns=_US_COLS)

    rows: list[dict[str, Any]] = []
    for o in orders:
        r = _order_us_row(o)
        if r:
            rows.append(r)
    if not rows:
        return pd.DataFrame(columns=_US_COLS)

    fin = pd.DataFrame(rows)
    fin["Date"] = pd.to_datetime(fin["Date"], errors="coerce")
    fin = fin.dropna(subset=["Date"])
    if fin.empty:
        return pd.DataFrame(columns=_US_COLS)
    fin["Month"] = fin["Date"].dt.to_period("M").astype(str)

    gfin = fin.groupby("Month", as_index=False).agg(
        Gross_merchandise=("Gross_merchandise", "sum"),
        Order_discounts=("Order_discounts", "sum"),
        Product_sales_net=("Product_sales_net", "sum"),
        Shipping_revenue=("Shipping_revenue", "sum"),
        Sales_tax_collected=("Sales_tax_collected", "sum"),
        Refunds_total=("Refunds_total", "sum"),
        Net_sales=("Net_sales", "sum"),
    )
    for c in gfin.columns:
        if c != "Month":
            gfin[c] = pd.to_numeric(gfin[c], errors="coerce").fillna(0.0).round(2)

    # COGS from line items by month
    cogs_m = pd.DataFrame(columns=["Month", "COGS"])
    if not orders_df.empty and "Date" in orders_df.columns and "Product_Cost" in orders_df.columns:
        od = orders_df.copy()
        od["Date"] = pd.to_datetime(od["Date"], errors="coerce")
        od = od.dropna(subset=["Date"])
        od["Month"] = od["Date"].dt.to_period("M").astype(str)
        cogs_m = (
            od.groupby("Month", as_index=False)["Product_Cost"]
            .sum()
            .rename(columns={"Product_Cost": "COGS"})
        )
        cogs_m["COGS"] = pd.to_numeric(cogs_m["COGS"], errors="coerce").fillna(0.0).round(2)

    # Marketing (Meta) by month — same basis as DAILY_SUMMARY
    mkt_m = pd.DataFrame(columns=["Month", "Marketing_advertising"])
    if not daily_final.empty and "Date" in daily_final.columns:
        dd = daily_final.copy()
        dd["Date"] = pd.to_datetime(dd["Date"], errors="coerce")
        dd = dd.dropna(subset=["Date"])
        dd["Month"] = dd["Date"].dt.to_period("M").astype(str)
        if "Ad_Spend" in dd.columns:
            mkt_m = (
                dd.groupby("Month", as_index=False)["Ad_Spend"]
                .sum()
                .rename(columns={"Ad_Spend": "Marketing_advertising"})
            )
            mkt_m["Marketing_advertising"] = (
                pd.to_numeric(mkt_m["Marketing_advertising"], errors="coerce").fillna(0.0).round(2)
            )

    out = gfin.merge(cogs_m, on="Month", how="outer").merge(mkt_m, on="Month", how="outer")
    out = out.sort_values("Month").reset_index(drop=True)
    for c in (
        "Gross_merchandise",
        "Order_discounts",
        "Product_sales_net",
        "Shipping_revenue",
        "Sales_tax_collected",
        "Refunds_total",
        "Net_sales",
        "COGS",
        "Marketing_advertising",
    ):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0).round(2)

    # Gross profit: net sales (subtotal + shipping − refunds) − COGS
    out["Gross_profit"] = (
        pd.to_numeric(out["Net_sales"], errors="coerce").fillna(0.0)
        - pd.to_numeric(out["COGS"], errors="coerce").fillna(0.0)
    ).round(2)
    out["Operating_income"] = (
        pd.to_numeric(out["Gross_profit"], errors="coerce").fillna(0.0)
        - pd.to_numeric(out["Marketing_advertising"], errors="coerce").fillna(0.0)
    ).round(2)

    out = out[_US_COLS].sort_values("Month").reset_index(drop=True)
    return out
