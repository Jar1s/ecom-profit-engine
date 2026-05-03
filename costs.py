"""Load supplier cost table from CSV or a Google Sheet tab."""

from __future__ import annotations

import logging
import math
import os
import re
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from normalize import (
    normalize_order_number,
    normalize_product_name,
    normalize_sku,
    product_title_family_levels,
)

if TYPE_CHECKING:
    from config import Settings

logger = logging.getLogger(__name__)
_NON_NUMERIC_COST_TAIL = re.compile(r"[^\d.,\-]+$")


def _sheet_header_key(raw: object) -> str:
    """Lowercase header for column detection, stripping invisible pasted characters."""
    return str(raw).strip().strip("\ufeff\u200b").lower()


def parse_supplier_cost_value(raw: object) -> float:
    """Parse supplier Cost from CSV/Sheets, including comma decimals and currency suffixes."""
    if raw is None or isinstance(raw, bool):
        return 0.0
    if isinstance(raw, (int, float)):
        if isinstance(raw, float) and (math.isnan(raw) or math.isinf(raw)):
            return 0.0
        return float(raw)
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "-", "—", "–"):
        return 0.0
    for sym in "$€£¥₹₽\u00a0":
        s = s.replace(sym, "")
    s = s.replace(" ", "").strip()
    s = _NON_NUMERIC_COST_TAIL.sub("", s)
    if not s:
        return 0.0
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2 and parts[1].isdigit():
            s = parts[0].replace(".", "") + "." + parts[1]
        else:
            s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


@dataclass(frozen=True)
class CostMaps:
    """Supplier unit costs: product title, exact SKU, SKU prefix (ITEM_CATALOG), order #, learned ORDERS_DB."""

    by_product: dict[str, float]
    by_sku: dict[str, float]
    by_order_single: dict[str, float] = field(default_factory=dict)
    learned_by_product_sku: dict[tuple[str, str], float] = field(default_factory=dict)
    # Longest-prefix-first: (normalized prefix, unit cost) for shared model / color variants
    sku_prefix_rules: tuple[tuple[str, float], ...] = ()
    # All title “levels” (full + stripped variants) → unit cost; same model, different color
    by_product_lineage: dict[str, float] = field(default_factory=dict)


def build_product_lineage_index(by_product: dict[str, float]) -> dict[str, float]:
    """
    For each supplier product title, register the same cost under every stripped variant
    (`` - farba / veľkosť`` removed stepwise) so Shopify lines with another color match.
    """
    index: dict[str, float] = {}
    for key, cost in by_product.items():
        if cost <= 0:
            continue
        for level in product_title_family_levels(key):
            if level not in index:
                index[level] = cost
            elif index[level] != cost:
                logger.warning(
                    "Zhoda základu názvu %r s rôznymi cenami %.4g vs %.4g — ponechávam prvú",
                    level,
                    index[level],
                    cost,
                )
    return index


def _cost_maps_from_dataframe(df: pd.DataFrame, *, source: str) -> CostMaps:
    """Build maps from Product + Cost and optional SKU (case-insensitive headers)."""
    col_lower = {_sheet_header_key(c): c for c in df.columns}
    if "product" not in col_lower or "cost" not in col_lower:
        raise ValueError(
            f"{source}: need columns Product and Cost (any casing). Got: {list(df.columns)}"
        )
    sku_col = col_lower.get("sku")
    rename = {
        col_lower["product"]: "Product",
        col_lower["cost"]: "Cost",
    }
    if sku_col:
        rename[sku_col] = "SKU"
        use = df.rename(columns=rename)[["Product", "Cost", "SKU"]]
    else:
        use = df.rename(columns=rename)[["Product", "Cost"]]
        use = use.assign(SKU="")

    by_product: dict[str, float] = {}
    by_sku: dict[str, float] = {}
    sku_only_rows = 0
    for _, row in use.iterrows():
        raw_name = str(row.get("Product", "") or "").strip()
        key = normalize_product_name(raw_name)
        cost = parse_supplier_cost_value(row.get("Cost", 0))
        raw_sku = str(row.get("SKU", "") or "").strip()
        sk = normalize_sku(raw_sku) if raw_sku else ""

        if key:
            if key in by_product:
                logger.warning("Duplicate cost row for normalized key %r — last wins", key)
            by_product[key] = cost
            if sk:
                if sk in by_sku and by_sku[sk] != cost:
                    logger.warning(
                        "Duplicate SKU %r with different costs in %s — last wins", sk, source
                    )
                by_sku[sk] = cost
        elif sk:
            sku_only_rows += 1
            if sk in by_sku and by_sku[sk] != cost:
                logger.warning(
                    "Duplicate SKU %r with different costs in %s — last wins", sk, source
                )
            by_sku[sk] = cost

    if sku_only_rows:
        logger.info(
            "Supplier costs %s: %s SKU-only row(s) registered on by_sku",
            source,
            sku_only_rows,
        )

    lineage = build_product_lineage_index(by_product)
    logger.info(
        "Loaded %s product costs (%s SKU overrides, %s lineage keys) from %s",
        len(by_product),
        len(by_sku),
        len(lineage),
        source,
    )
    return CostMaps(
        by_product=by_product,
        by_sku=by_sku,
        by_product_lineage=lineage,
    )


def _load_by_order_single_from_sheet(settings: "Settings", tab: str) -> dict[str, float]:
    """``OrderNo`` → unit cost from BillDetail single-line rows."""
    from sheets import try_read_worksheet_dataframe

    df = try_read_worksheet_dataframe(settings, tab)
    if df is None or df.empty:
        return {}
    col_lower = {str(c).strip().lower(): c for c in df.columns}
    on_c = col_lower.get("orderno") or col_lower.get("order_no")
    cost_c = col_lower.get("unitcost") or col_lower.get("unit_cost") or col_lower.get("cost")
    if not on_c or not cost_c:
        logger.warning(
            "Tab %r: expected columns OrderNo and UnitCost, got %s", tab, list(df.columns)
        )
        return {}
    out: dict[str, float] = {}
    for _, row in df.iterrows():
        ok = normalize_order_number(row.get(on_c))
        if not ok:
            continue
        try:
            v = float(row.get(cost_c) or 0)
        except (TypeError, ValueError):
            continue
        if ok in out and out[ok] != v:
            logger.warning("Duplicate OrderNo %r in %r — last wins", ok, tab)
        out[ok] = v
    logger.info("Loaded %s single-item order cost rows from %r", len(out), tab)
    return out


def _learned_unit_costs_from_orders_sheet(settings: "Settings") -> dict[tuple[str, str], float]:
    """
    Median unit cost (Product_Cost / Quantity) from ORDERS_DB for (product_key, sku_key).
    sku_key is normalized SKU or empty string.
    """
    from sheets import try_read_worksheet_dataframe

    tab = os.getenv("SHEET_TAB_ORDERS_DB", "ORDERS_DB").strip() or "ORDERS_DB"
    df = try_read_worksheet_dataframe(
        settings,
        tab,
        required_headers=["Product", "Quantity", "Product_Cost"],
    )
    if df is None or df.empty:
        return {}
    col_lower = {str(c).strip().lower(): c for c in df.columns}
    need = ("product", "quantity", "product_cost")
    if not all(c in col_lower for c in need):
        logger.warning(
            "Learn costs: tab %r needs Product, Quantity, Product_Cost — got %s",
            tab,
            list(df.columns),
        )
        return {}
    p_col = col_lower["product"]
    q_col = col_lower["quantity"]
    c_col = col_lower["product_cost"]
    sku_col = col_lower.get("sku")

    buckets: dict[tuple[str, str], list[float]] = {}
    for _, row in df.iterrows():
        try:
            qty = float(row.get(q_col) or 0)
            pc = float(row.get(c_col) or 0)
        except (TypeError, ValueError):
            continue
        if qty <= 0 or pc <= 0:
            continue
        pk = normalize_product_name(str(row.get(p_col) or ""))
        if not pk:
            continue
        sk_raw = str(row.get(sku_col) or "").strip() if sku_col else ""
        sk = normalize_sku(sk_raw) if sk_raw else ""
        unit = pc / qty
        buckets.setdefault((pk, sk), []).append(unit)

    out: dict[tuple[str, str], float] = {}
    for key, vals in buckets.items():
        if not vals:
            continue
        out[key] = round(float(statistics.median(vals)), 4)

    logger.info("Learned %s unit-cost keys from tab %r", len(out), tab)
    return out


def _load_item_catalog_prefix_rules(settings: "Settings", tab: str) -> tuple[tuple[str, float], ...]:
    """
    Human-edited catalog: ``SKU_Prefix`` + ``UnitCost`` — line-item SKU starting with
    prefix gets that unit cost (longest matching prefix wins). Use for same model, different color.
    """
    from sheets import try_read_worksheet_dataframe

    df = try_read_worksheet_dataframe(settings, tab)
    if df is None or df.empty:
        return ()
    col_lower = {str(c).strip().lower(): c for c in df.columns}
    pref_c = col_lower.get("sku_prefix") or col_lower.get("parent_sku")
    cost_c = (
        col_lower.get("unitcost")
        or col_lower.get("unit_cost")
        or col_lower.get("cost")
    )
    if not pref_c or not cost_c:
        logger.warning(
            "ITEM_CATALOG tab %r: expected SKU_Prefix (or Parent_SKU) and UnitCost — got %s",
            tab,
            list(df.columns),
        )
        return ()
    by_pref: dict[str, float] = {}
    for _, row in df.iterrows():
        raw_p = str(row.get(pref_c) or "").strip()
        if not raw_p:
            continue
        p = normalize_sku(raw_p)
        if not p:
            continue
        try:
            c = float(row.get(cost_c) or 0)
        except (TypeError, ValueError):
            continue
        if c <= 0:
            continue
        by_pref[p] = c
    if not by_pref:
        return ()
    rules = sorted(by_pref.items(), key=lambda x: (-len(x[0]), x[0]))
    out = tuple(rules)
    logger.info("Loaded %s ITEM_CATALOG prefix rows from %r", len(out), tab)
    return out


def _merge_auxiliary_sheet_costs(settings: "Settings", base: CostMaps) -> CostMaps:
    """Attach item catalog, BillDetail order-single map, and/or learned ORDERS_DB costs."""
    by_order = dict(base.by_order_single)
    learned = dict(base.learned_by_product_sku)
    prefix_rules = tuple(base.sku_prefix_rules)

    if settings.item_catalog_sheet_tab:
        pr = _load_item_catalog_prefix_rules(settings, settings.item_catalog_sheet_tab)
        if pr:
            prefix_rules = pr

    if settings.supplier_bill_single_orders_tab:
        by_order.update(
            _load_by_order_single_from_sheet(settings, settings.supplier_bill_single_orders_tab)
        )
    if settings.learn_costs_from_orders_sheet:
        learned.update(_learned_unit_costs_from_orders_sheet(settings))

    if (
        by_order == dict(base.by_order_single)
        and learned == dict(base.learned_by_product_sku)
        and prefix_rules == tuple(base.sku_prefix_rules)
    ):
        return base
    return CostMaps(
        by_product=base.by_product,
        by_sku=base.by_sku,
        by_order_single=by_order,
        learned_by_product_sku=learned,
        sku_prefix_rules=prefix_rules,
        by_product_lineage=base.by_product_lineage,
    )


def load_cost_maps(settings: "Settings") -> CostMaps:
    """
    Supplier costs: worksheet in the same spreadsheet as the pipeline (default tab name
    ``SUPPLIER_COSTS``), unless ``SUPPLIER_COSTS_FROM_CSV`` is set — then CSV file is used.
    Optional ``SKU`` column enables matching when Shopify product titles differ from BillDetail.

    When the spreadsheet is configured, also loads (if present / enabled):

    - ``SUPPLIER_BILL_SINGLE_ORDERS_TAB``: order number → unit cost for BillDetail rows with
      exactly one line item (used when the Shopify order has exactly one line item).
    - ``LEARN_COSTS_FROM_ORDERS_SHEET``: median unit cost from tab ``SHEET_TAB_ORDERS_DB``.
    - ``ITEM_CATALOG_SHEET_TAB``: optional ``SKU_Prefix`` / ``UnitCost`` rows — same wholesale price
      for all variants whose SKU starts with that prefix (e.g. one row ``FCGP42825`` → all ``FCGP42825.*``).
    """
    if settings.supplier_costs_sheet_tab:
        cm = _load_cost_maps_from_google_sheet(settings)
    else:
        cm = _load_cost_maps_from_csv(settings.supplier_csv_path)

    if settings.google_sheet_id or settings.google_sheet_name:
        cm = _merge_auxiliary_sheet_costs(settings, cm)
    return cm


def _load_cost_maps_from_csv(csv_path: Path) -> CostMaps:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Supplier costs CSV not found: {csv_path}")
    df = pd.read_csv(csv_path)
    return _cost_maps_from_dataframe(df, source=str(csv_path))


def _load_cost_maps_from_google_sheet(settings: "Settings") -> CostMaps:
    from sheets import get_or_create_supplier_costs_worksheet

    tab = (settings.supplier_costs_sheet_tab or "").strip()
    if not tab:
        raise RuntimeError("supplier_costs_sheet_tab is empty")

    ws = get_or_create_supplier_costs_worksheet(settings, tab)

    values = ws.get_all_values()
    if not values:
        logger.warning("Supplier sheet %r is empty", tab)
        return CostMaps(by_product={}, by_sku={}, by_product_lineage={})

    header = [str(c).strip() for c in values[0]]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    return _cost_maps_from_dataframe(df, source=f"Google Sheet tab {tab!r}")


def load_cost_map_from_path(csv_path: Path) -> CostMaps:
    """Load from CSV path (tests, scripts)."""
    return _load_cost_maps_from_csv(csv_path)
