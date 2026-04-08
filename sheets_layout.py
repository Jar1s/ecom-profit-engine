"""Build Google Sheet cell grids: summary block + table + header row index for formatting."""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd

from config import Settings

SheetKind = Literal["orders", "order_level", "meta", "meta_campaigns", "daily", "bookkeeping"]

# Summary rows need enough columns; narrow dataframes (e.g. META_DATA with 3 cols) would otherwise
# truncate _pad(..., row) and drop trailing cells (including local-currency totals).
_MIN_GRID_COLS = 8


def _pad(width: int, row: list[Any]) -> list[Any]:
    r = list(row)
    while len(r) < width:
        r.append("")
    return r[:width]


def _df_to_value_rows(df: pd.DataFrame, width: int) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for _, row in df.iterrows():
        out: list[Any] = []
        for v in row.tolist():
            if pd.isna(v):
                out.append("")
            else:
                out.append(v)
        rows.append(_pad(width, out))
    return rows


def sheet_values_with_summary(
    df: pd.DataFrame,
    *,
    kind: SheetKind,
    settings: Settings,
) -> tuple[list[list[Any]], int]:
    """
    Return (all values top-to-bottom, 1-based row number of the header row).
    """
    if df.empty:
        header = list(df.columns) if len(df.columns) else ["(no data)"]
    else:
        header = [str(c) for c in df.columns]
    width = max(len(header), _MIN_GRID_COLS)

    cur = settings.report_currency.strip() or "—"
    rate = settings.usd_per_local
    rate_s = f"{rate:.6g}" if rate else "—"

    summary: list[list[Any]] = []

    if kind == "orders":
        title = "📊 Detail položiek (line items)"
        n = len(df)
        rev = float(df["Revenue"].sum()) if "Revenue" in df.columns else 0.0
        cogs = float(df["Product_Cost"].sum()) if "Product_Cost" in df.columns else 0.0
        gp = float(df["Gross_Profit"].sum()) if "Gross_Profit" in df.columns else 0.0
        summary.append(
            _pad(
                width,
                [
                    title,
                    f"Mena obchodu: {cur}",
                    f"USD za 1 jednotku meny: {rate_s}",
                    "",
                    "",
                    "",
                    "",
                    "",
                ],
            )
        )
        summary.append(
            _pad(
                width,
                [
                    "Súčty",
                    f"Počet riadkov: {n}",
                    "Revenue",
                    round(rev, 2),
                    "Product_Cost",
                    round(cogs, 2),
                    "Gross_Profit",
                    round(gp, 2),
                ],
            )
        )
        if rate and "Revenue_USD" in df.columns:
            summary.append(
                _pad(
                    width,
                    [
                        "Súčty USD",
                        "",
                        "Revenue_USD",
                        round(float(df["Revenue_USD"].sum()), 2),
                        "Product_Cost_USD",
                        round(float(df["Product_Cost_USD"].sum()), 2),
                        "Gross_Profit_USD",
                        round(float(df["Gross_Profit_USD"].sum()), 2),
                    ],
                )
            )
        elif not rate:
            summary.append(
                _pad(
                    width,
                    [
                        "Tip",
                        "Pre stĺpce *_USD nastav USD_PER_LOCAL_UNIT (napr. 0.65 ak 1 AUD ≈ 0.65 USD).",
                        "",
                        "",
                        "",
                        "",
                    ],
                )
            )
        else:
            summary.append(_pad(width, [""]))

    elif kind == "order_level":
        title = "📊 Súhrn objednávok"
        n = len(df)
        rev = float(df["Revenue"].sum()) if "Revenue" in df.columns else 0.0
        cogs = float(df["Product_Cost"].sum()) if "Product_Cost" in df.columns else 0.0
        gp = float(df["Gross_Profit"].sum()) if "Gross_Profit" in df.columns else 0.0
        summary.append(
            _pad(
                width,
                [title, f"Mena: {cur}", f"USD/1: {rate_s}", "", "", "", "", ""],
            )
        )
        summary.append(
            _pad(
                width,
                [
                    "Súčty",
                    f"Objednávok: {n}",
                    "Revenue",
                    round(rev, 2),
                    "Product_Cost",
                    round(cogs, 2),
                    "Gross_Profit",
                    round(gp, 2),
                ],
            )
        )
        if rate and "Revenue_USD" in df.columns:
            summary.append(
                _pad(
                    width,
                    [
                        "Súčty USD",
                        "",
                        "Revenue_USD",
                        round(float(df["Revenue_USD"].sum()), 2),
                        "Product_Cost_USD",
                        round(float(df["Product_Cost_USD"].sum()), 2),
                        "Gross_Profit_USD",
                        round(float(df["Gross_Profit_USD"].sum()), 2),
                    ],
                )
            )
        else:
            summary.append(_pad(width, [""]))

    elif kind == "meta":
        title = "📊 Meta Ads — denné výdavky (USD)"
        n = len(df)
        spend_usd = float(df["Ad_Spend_USD"].sum()) if "Ad_Spend_USD" in df.columns else 0.0
        summary.append(
            _pad(
                width,
                [title, "Mena reportu: USD", "", "", "", "", "", ""],
            )
        )
        summary.append(
            _pad(
                width,
                [
                    "Súčty",
                    f"Dní v tabuľke: {n}",
                    "Ad_Spend_USD",
                    round(spend_usd, 2),
                    "",
                    "",
                    "",
                    "",
                ],
            )
        )
        summary.append(_pad(width, [""]))

    elif kind == "meta_campaigns":
        title = "📊 Meta — kampane × deň (spend + konverzie)"
        n = len(df)
        spend = float(df["Ad_Spend"].sum()) if "Ad_Spend" in df.columns else 0.0
        pur = float(df["Purchases"].sum()) if "Purchases" in df.columns else 0.0
        pv = float(df["Purchase_Value"].sum()) if "Purchase_Value" in df.columns else 0.0
        atc = (
            float(df["Adds_to_Cart"].sum())
            if "Adds_to_Cart" in df.columns
            else 0.0
        )
        ichk = (
            float(df["Checkouts_Initiated"].sum())
            if "Checkouts_Initiated" in df.columns
            else 0.0
        )
        summary.append(
            _pad(
                width,
                [
                    title,
                    "Spend a konverzie: mena reklamného účtu (Meta), nie Shopify",
                    f"USD/1 (Shopify): {rate_s}",
                    "",
                    "",
                    "",
                    "",
                    "",
                ],
            )
        )
        summary.append(
            _pad(
                width,
                [
                    "Súčty",
                    f"Riadkov: {n}",
                    "Ad_Spend",
                    round(spend, 2),
                    "Purchases",
                    round(pur, 2),
                    "Purchase_Value",
                    round(pv, 2),
                    "Adds_to_Cart",
                    round(atc, 2),
                    "Checkouts_Initiated",
                    round(ichk, 2),
                    "",
                ],
            )
        )
        if rate and "Ad_Spend_USD" in df.columns:
            pv_usd = (
                float(df["Purchase_Value_USD"].sum())
                if "Purchase_Value_USD" in df.columns
                else 0.0
            )
            summary.append(
                _pad(
                    width,
                    [
                        "Ad_Spend USD",
                        round(float(df["Ad_Spend_USD"].sum()), 2),
                        "Purchase_Value USD",
                        round(pv_usd, 2),
                        "",
                        "",
                        "",
                        "",
                        "",
                    ],
                )
            )
        else:
            summary.append(_pad(width, [""]))

    elif kind == "daily":
        usd_only = settings.daily_summary_usd_primary and "Net_Profit" in df.columns
        n = len(df)
        rev = float(df["Revenue"].sum()) if "Revenue" in df.columns else 0.0
        cogs = float(df["Product_Cost"].sum()) if "Product_Cost" in df.columns else 0.0
        gp = float(df["Gross_Profit"].sum()) if "Gross_Profit" in df.columns else 0.0
        ads = 0.0
        if "Ad_Spend" in df.columns:
            ads = float(df["Ad_Spend"].fillna(0).sum())

        if usd_only:
            net = float(df["Net_Profit"].sum()) if "Net_Profit" in df.columns else 0.0
            title = "📊 Denný prehľad (USD) — tržby, COGS, reklama"
            summary.append(
                _pad(
                    width,
                    [
                        title,
                        "Všetky sumy v USD (Shopify × kurz; Meta po zlúčení v USD).",
                        f"Kurz v config: USD_PER_LOCAL_UNIT={rate_s} · REPORT_CURRENCY={cur}",
                        "Net_Profit = Gross_Profit − Ad_Spend (nie je to kompletný účtovný výsledok).",
                        "",
                        "",
                        "",
                        "",
                    ],
                )
            )
            summary.append(
                _pad(
                    width,
                    [
                        "Súčty (USD, všetky dni)",
                        f"Dní: {n}",
                        "Revenue",
                        round(rev, 2),
                        "Product_Cost",
                        round(cogs, 2),
                        "Gross_Profit",
                        round(gp, 2),
                    ],
                )
            )
            summary.append(
                _pad(
                    width,
                    [
                        "Ad_Spend",
                        round(ads, 2),
                        "Net_Profit",
                        round(net, 2),
                        "",
                        "",
                        "",
                        "",
                    ],
                )
            )
        else:
            title = "📊 Denný prehľad + Meta (P&L)"
            summary.append(
                _pad(
                    width,
                    [title, f"Mena: {cur}", f"USD/1: {rate_s}", "", "", "", "", ""],
                )
            )
            summary.append(
                _pad(
                    width,
                    [
                        "Súčty (všetky dni)",
                        f"Dní: {n}",
                        "Revenue",
                        round(rev, 2),
                        "Product_Cost",
                        round(cogs, 2),
                        "Gross_Profit",
                        round(gp, 2),
                    ],
                )
            )
            if rate and "Revenue_USD" in df.columns:
                ad_usd = (
                    float(df["Ad_Spend_USD"].fillna(0).sum()) if "Ad_Spend_USD" in df.columns else 0.0
                )
                summary.append(
                    _pad(
                        width,
                        [
                            "Meta Ad_Spend (miestna)",
                            round(ads, 2),
                            "Revenue_USD",
                            round(float(df["Revenue_USD"].sum()), 2),
                            "Product_Cost_USD",
                            round(float(df["Product_Cost_USD"].sum()), 2),
                            "Gross_Profit_USD",
                            round(float(df["Gross_Profit_USD"].sum()), 2),
                            "Ad_Spend USD",
                            round(ad_usd, 2),
                        ],
                    )
                )
            else:
                summary.append(
                    _pad(
                        width,
                        [
                            "Meta Ad_Spend",
                            round(ads, 2),
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                        ],
                    )
                )

    elif kind == "bookkeeping":
        n = len(df)
        cur = settings.report_currency.strip() or "—"
        rate = settings.usd_per_local
        rate_s = f"{rate:.6g}" if rate else "—"
        usd_note = (
            "Amounts in shop currency; with DAILY_SUMMARY USD primary + USD_PER_LOCAL_UNIT, "
            "daily merge matches Meta — BOOKKEEPING order fields stay in shop currency."
            if settings.daily_summary_usd_primary and rate
            else f"Amounts in shop/report currency ({cur}). US-oriented management P&L."
        )

        def _sum(col: str) -> float:
            if col not in df.columns:
                return 0.0
            return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())

        net_sales = _sum("Net_sales")
        cogs = _sum("COGS")
        gp = _sum("Gross_profit")
        mkt = _sum("Marketing_advertising")
        op = _sum("Operating_income")
        tax_coll = _sum("Sales_tax_collected")

        summary.append(
            _pad(
                width,
                [
                    "📒 Bookkeeping (US) — monthly management P&L",
                    usd_note,
                    "Not tax or legal advice. Sales tax shown for reference (pass-through). "
                    "Refunds = sum of refund transactions in Shopify. Use a CPA for federal/state filings.",
                    f"Orders: Shopify REST · COGS: pipeline line items · Ads: Meta (DAILY_SUMMARY). "
                    f"REPORT_CURRENCY={cur} · USD_PER_LOCAL_UNIT={rate_s}",
                    "",
                    "",
                    "",
                    "",
                ],
            )
        )
        summary.append(
            _pad(
                width,
                [
                    "Totals (all months in table)",
                    f"Month rows: {n}",
                    "Net_sales",
                    round(net_sales, 2),
                    "COGS",
                    round(cogs, 2),
                    "Gross_profit",
                    round(gp, 2),
                ],
            )
        )
        summary.append(
            _pad(
                width,
                [
                    "Marketing_advertising",
                    round(mkt, 2),
                    "Operating_income",
                    round(op, 2),
                    "Sales_tax_collected (ref.)",
                    round(tax_coll, 2),
                    "",
                    "",
                    "",
                ],
            )
        )
        summary.append(_pad(width, [""]))

    else:
        raise ValueError(f"Unknown sheet layout kind: {kind!r}")

    summary.append(_pad(width, [""] * width))
    header_row_index = len(summary) + 1
    header_row = _pad(width, header)
    data_rows = _df_to_value_rows(df, width) if not df.empty else []
    all_values = summary + [header_row] + data_rows
    return all_values, header_row_index


def sheet_values_plain(df: pd.DataFrame) -> tuple[list[list[Any]], int]:
    """No summary block; header is row 1."""
    values = []
    if df.empty:
        header = list(df.columns) if len(df.columns) else ["(no data)"]
    else:
        header = [str(c) for c in df.columns]
    width = len(header)
    values.append(_pad(width, header))
    values.extend(_df_to_value_rows(df, width))
    return values, 1
