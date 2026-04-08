"""Reports for incomplete pipeline data (e.g. missing supplier unit costs)."""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

_MISSING_COLUMNS = [
    "Product",
    "SKU",
    "Line_Items",
    "Revenue_Sum",
    "First_Date",
    "Last_Date",
]


def build_missing_supplier_costs_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rows where ``Product_Cost`` is 0 or missing, aggregated by Product + SKU
    (sorted by number of line items descending).
    """
    if df.empty or "Product_Cost" not in df.columns:
        return pd.DataFrame(columns=_MISSING_COLUMNS)
    pc = pd.to_numeric(df["Product_Cost"], errors="coerce").fillna(0.0)
    miss = df.loc[pc <= 0].copy()
    if miss.empty:
        return pd.DataFrame(columns=_MISSING_COLUMNS)

    if "SKU" not in miss.columns:
        miss["SKU"] = ""
    miss["SKU"] = miss["SKU"].fillna("").astype(str)

    g = (
        miss.groupby(["Product", "SKU"], dropna=False)
        .agg(
            Line_Items=("Order", "count"),
            Revenue_Sum=("Revenue", "sum"),
            First_Date=("Date", "min"),
            Last_Date=("Date", "max"),
        )
        .reset_index()
    )
    g["Revenue_Sum"] = g["Revenue_Sum"].round(2)
    g = g.sort_values("Line_Items", ascending=False)
    g.columns = _MISSING_COLUMNS
    return g


def log_missing_supplier_costs(
    orders_df: pd.DataFrame,
    report: pd.DataFrame,
    *,
    sheet_tab: str | None,
) -> None:
    """Emit INFO / WARNING with counts; sheet_tab is the worksheet name if configured."""
    if orders_df.empty or "Product_Cost" not in orders_df.columns:
        return
    n_lines = int((pd.to_numeric(orders_df["Product_Cost"], errors="coerce").fillna(0.0) <= 0).sum())
    n_unique = len(report.index)
    tab_s = sheet_tab or "(záložka vypnutá)"
    if n_lines == 0:
        logger.info(
            "data_quality: všetky položky majú doplnený Product_Cost (%s riadkov).",
            len(orders_df),
        )
        return
    logger.warning(
        "data_quality: %s riadkov bez supplier nákladu (Product_Cost=0), "
        "%s unikátnych kombinácií Product+SKU — skontroluj záložku %s a SUPPLIER_COSTS / BillDetail.",
        n_lines,
        n_unique,
        tab_s,
    )
