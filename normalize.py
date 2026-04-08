"""Normalize product titles for supplier cost matching."""

from __future__ import annotations

import math
import re

_MULTI_SPACE = re.compile(r"\s+")


def normalize_product_name(name: str) -> str:
    """Trim, collapse whitespace, casefold for case-insensitive lookup."""
    if not name:
        return ""
    s = name.strip()
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
