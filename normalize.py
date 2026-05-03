"""Normalize product titles for supplier cost matching."""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta
from typing import Any

# Columns written/read as calendar YYYY-MM-DD (never leave raw Sheets serials in cells).
SHEET_DATE_COLUMN_NAMES: frozenset[str] = frozenset(("Date", "Shipped_Date"))

_MULTI_SPACE = re.compile(r"\s+")


def normalize_product_name(name: str) -> str:
    """Trim, collapse whitespace, casefold for case-insensitive lookup."""
    if not name:
        return ""
    s = str(name).strip()
    # NBSP and thin spaces → regular space before collapse
    s = s.replace("\u00a0", " ").replace("\u2009", " ").replace("\u202f", " ")
    # Shopify/CMS often use en/em dash instead of " - " between model name and color (Rose vs Beige)
    s = re.sub(r"\s+[–—−]\s+", " - ", s)
    # Tight: Pullover–Rose / Pullover– Rose (unicode dash)
    s = re.sub(r"([^\s])[–—−]([^\s])", r"\1 - \2", s)
    s = re.sub(r"([^\s])[–—−]\s+", r"\1 - ", s)
    s = re.sub(r"\s+[–—−]([^\s])", r" - \1", s)
    s = _MULTI_SPACE.sub(" ", s)
    return s.casefold()


def product_title_family_levels(normalized: str) -> list[str]:
    """
    Full normalized title, then repeatedly without the last `` - …`` segment.
    Used so the same model (e.g. poncho) with different color/size endings can share
    one wholesale price from SUPPLIER_COSTS / BillDetail import.
    Input must already be :func:`normalize_product_name` output.
    """
    if not normalized:
        return []
    out: list[str] = []
    seen: set[str] = set()
    n = normalized
    while n:
        if n not in seen:
            seen.add(n)
            out.append(n)
        if " - " not in n:
            break
        n = n.rsplit(" - ", 1)[0].strip()
    return out


def normalize_sku(sku: str) -> str:
    """Strip and uppercase for BillDetail ↔ Shopify SKU matching."""
    if not sku:
        return ""
    return str(sku).strip().upper()


def normalize_order_number(order_ref: object) -> str:
    """
    Stable key for BillDetail OrderNo ↔ Shopify order name (e.g. #1053).
    Handles numeric Excel cells and strings with digits.
    """
    if order_ref is None:
        return ""
    if isinstance(order_ref, float) and (math.isnan(order_ref)):
        return ""
    if isinstance(order_ref, (int, float)) and not isinstance(order_ref, bool):
        f = float(order_ref)
        if math.isfinite(f) and abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
        return "".join(re.findall(r"\d+", str(order_ref))) or ""
    s = str(order_ref).strip()
    if not s:
        return ""
    try:
        f = float(s)
        if math.isfinite(f) and abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
    except ValueError:
        pass
    digits = "".join(re.findall(r"\d+", s))
    return digits


def sheet_date_to_iso(value: Any) -> str:
    """
    One cell ``Date`` / ``Shipped_Date`` → ``YYYY-MM-DD`` string for Sheets and merges.

    Google Sheets sometimes stores calendar days as numeric serials (~40k–55k). Exporting
    ``pd.Timestamp`` raw can also turn into serials in the UI. Always prefer ISO text.
    """
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        dt_date = getattr(value, "date", None)
        if callable(dt_date):
            d = dt_date()
            if isinstance(d, date):
                return d.isoformat()
    except Exception:
        pass
    s = str(value).strip()
    if not s or s.lower() in ("nat", "none", "nan"):
        return ""
    if len(s) >= 10 and s[4] in "-/" and s[7] in "-/":
        return s[:10].replace("/", "-")
    try:
        xf = float(s)
        if math.isnan(xf):
            return ""
        if abs(xf) < 1e-9:
            return ""
        # Google Sheets / Excel day serials (roughly 1954–2078); wider than before for old ships.
        if 20000 <= xf <= 65000:
            base = date(1899, 12, 30)
            return (base + timedelta(days=int(round(xf)))).isoformat()
    except ValueError:
        pass
    return s[:10] if len(s) >= 10 else ""
