"""Build Google Sheet cell grids: summary block + table + header row index for formatting."""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd

from config import Settings

SheetKind = Literal["orders", "order_level", "meta", "meta_campaigns", "daily"]

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
        title = "📊 Meta Ads — denné výdavky"
        n = len(df)
        spend = float(df["Ad_Spend"].sum()) if "Ad_Spend" in df.columns else 0.0
        summary.append(
            _pad(
                width,
                [title, f"Mena účtu: {cur}", f"USD/1: {rate_s}", "", "", "", "", ""],
            )
        )
        spend_lbl = f"Ad_Spend ({cur})" if cur and cur != "—" else "Ad_Spend"
        summary.append(
            _pad(
                width,
                [
                    "Súčty",
                    f"Dní v tabuľke: {n}",
                    spend_lbl,
                    round(spend, 2),
                    "",
                    "",
                    "",
                    "",
                ],
            )
        )
        if rate and "Ad_Spend_USD" in df.columns:
            summary.append(
                _pad(
                    width,
                    [
                        "Ad_Spend USD",
                        round(float(df["Ad_Spend_USD"].sum()), 2),
                        "",
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

    elif kind == "meta_campaigns":
        title = "📊 Meta — kampane × deň (spend + konverzie)"
        n = len(df)
        spend = float(df["Ad_Spend"].sum()) if "Ad_Spend" in df.columns else 0.0
        pur = float(df["Purchases"].sum()) if "Purchases" in df.columns else 0.0
        pv = float(df["Purchase_Value"].sum()) if "Purchase_Value" in df.columns else 0.0
        summary.append(
            _pad(
                width,
                [title, f"Mena účtu: {cur}", f"USD/1: {rate_s}", "", "", "", "", ""],
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

    else:  # daily
        title = "📊 Denný prehľad + Meta (P&L)"
        n = len(df)
        rev = float(df["Revenue"].sum()) if "Revenue" in df.columns else 0.0
        cogs = float(df["Product_Cost"].sum()) if "Product_Cost" in df.columns else 0.0
        gp = float(df["Gross_Profit"].sum()) if "Gross_Profit" in df.columns else 0.0
        ads = 0.0
        if "Ad_Spend" in df.columns:
            ads = float(df["Ad_Spend"].fillna(0).sum())
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
                        "Meta Ad_Spend",
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
