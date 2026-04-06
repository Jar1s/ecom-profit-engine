"""Load supplier cost table from CSV."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from normalize import normalize_product_name

logger = logging.getLogger(__name__)


def load_cost_map(csv_path: Path) -> dict[str, float]:
    """
    Map normalized product name -> unit cost (same currency as revenue).
    Missing or invalid costs default to 0 at join time in transform.
    """
    if not csv_path.is_file():
        raise FileNotFoundError(f"Supplier costs CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if "Product" not in df.columns or "Cost" not in df.columns:
        raise ValueError("supplier_costs.csv must contain columns: Product, Cost")

    result: dict[str, float] = {}
    for _, row in df.iterrows():
        raw_name = str(row.get("Product", "") or "").strip()
        key = normalize_product_name(raw_name)
        if not key:
            continue
        try:
            cost = float(row.get("Cost", 0) or 0)
        except (TypeError, ValueError):
            cost = 0.0
        if key in result:
            logger.warning("Duplicate cost row for normalized key %r — last wins", key)
        result[key] = cost

    logger.info("Loaded %s product costs from %s", len(result), csv_path)
    return result
