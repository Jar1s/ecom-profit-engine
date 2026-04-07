"""Parse supplier BillDetail Excel exports into Product / Cost (for pipeline or Sheets)."""

from __future__ import annotations

import io
import re
from collections import defaultdict

import pandas as pd

from normalize import normalize_product_name

# One line item: «SKU,Title…:qty(unitOrLinePrice)» — title may contain commas.
_SEGMENT = re.compile(
    r"^([^,]+),(.+):(\d+)\(([\d.]+)\)\s*$",
    re.DOTALL,
)


def _segments(product_info: str) -> list[str]:
    s = str(product_info).strip()
    if not s:
        return []
    return [p.strip() for p in s.split(";") if p.strip()]


def find_bill_detail_columns(df: pd.DataFrame) -> tuple[str, str]:
    lower = {str(c).strip().lower(): c for c in df.columns}
    pi = lower.get("productinfo") or lower.get("product info")
    am = lower.get("amount")
    if not pi or not am:
        raise ValueError(
            "BillDetail export needs columns ProductInfo and Amount. "
            f"Found: {list(df.columns)}"
        )
    return pi, am


def _parse_segment(segment: str) -> tuple[str, float] | None:
    m = _SEGMENT.match(segment.strip())
    if not m:
        return None
    _sku, title, qty_s, price_s = m.groups()
    qty = max(1, int(qty_s))
    try:
        line_or_unit = float(price_s)
    except (TypeError, ValueError):
        line_or_unit = 0.0
    unit = line_or_unit / qty
    return title.strip(), round(unit, 4)


def iter_parsed_lines(
    product_info: str, row_amount: float | None
) -> list[tuple[str, float]]:
    if product_info is None or (
        isinstance(product_info, float) and pd.isna(product_info)
    ):
        return []
    s = str(product_info).strip()
    segs = _segments(s)
    if not segs:
        return []
    parsed_all = [_parse_segment(seg) for seg in segs]
    parsed = [p for p in parsed_all if p is not None]
    if parsed:
        return parsed
    m2 = _SEGMENT.match(s)
    if not m2:
        return []
    _sku, title, qty_s, price_s = m2.groups()
    qty = max(1, int(qty_s))
    try:
        p = float(price_s)
    except (TypeError, ValueError):
        p = 0.0
    try:
        amt = float(row_amount) if row_amount is not None else 0.0
    except (TypeError, ValueError):
        amt = 0.0
    unit = p / qty if p else (amt / qty if amt else 0.0)
    return [(title.strip(), round(unit, 4))]


def read_bill_detail_sheet(content: bytes, filename: str) -> pd.DataFrame:
    """Load the BillDetail tab from .xls / .xlsx bytes."""
    buf = io.BytesIO(content)
    name = (filename or "export.xls").lower()
    if name.endswith(".xlsx"):
        engine = "openpyxl"
    else:
        engine = "xlrd"
    xl = pd.ExcelFile(buf, engine=engine)
    sheet = "BillDetail" if "BillDetail" in xl.sheet_names else xl.sheet_names[0]
    return xl.parse(sheet)


def bill_detail_dataframe_to_supplier_costs(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate BillDetail rows to unique Product + median Cost."""
    pi_col, am_col = find_bill_detail_columns(df)
    costs_by_norm: dict[str, list[float]] = defaultdict(list)
    display_by_norm: dict[str, str] = {}

    for _, row in df.iterrows():
        raw_am = row.get(am_col)
        try:
            row_amount = (
                float(raw_am) if raw_am is not None and str(raw_am) != "nan" else None
            )
        except (TypeError, ValueError):
            row_amount = None
        for title, unit in iter_parsed_lines(row.get(pi_col), row_amount):
            key = normalize_product_name(title)
            if not key:
                continue
            costs_by_norm[key].append(unit)
            display_by_norm.setdefault(key, title)

    rows: list[tuple[str, float]] = []
    for key in sorted(costs_by_norm.keys()):
        vals = sorted(costs_by_norm[key])
        mid = vals[len(vals) // 2]
        rows.append((display_by_norm[key], round(mid, 2)))

    return pd.DataFrame(rows, columns=["Product", "Cost"])


def bill_detail_bytes_to_supplier_costs_df(content: bytes, filename: str) -> pd.DataFrame:
    """Parse uploaded file bytes → supplier costs DataFrame."""
    raw = read_bill_detail_sheet(content, filename)
    out = bill_detail_dataframe_to_supplier_costs(raw)
    if out.empty:
        raise ValueError(
            "No product lines parsed. Check ProductInfo format (SKU,Title:qty(price))."
        )
    return out
