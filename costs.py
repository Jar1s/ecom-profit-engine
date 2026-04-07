"""Load supplier cost table from CSV or a Google Sheet tab."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import gspread.exceptions
import pandas as pd

from normalize import normalize_product_name

if TYPE_CHECKING:
    from config import Settings

logger = logging.getLogger(__name__)


def _cost_map_from_dataframe(df: pd.DataFrame, *, source: str) -> dict[str, float]:
    """Build map from columns Product and Cost (case-insensitive header names)."""
    col_lower = {str(c).strip().lower(): c for c in df.columns}
    if "product" not in col_lower or "cost" not in col_lower:
        raise ValueError(
            f"{source}: need columns Product and Cost (any casing). Got: {list(df.columns)}"
        )
    use = df.rename(
        columns={
            col_lower["product"]: "Product",
            col_lower["cost"]: "Cost",
        }
    )[["Product", "Cost"]]

    result: dict[str, float] = {}
    for _, row in use.iterrows():
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

    logger.info("Loaded %s product costs from %s", len(result), source)
    return result


def load_cost_map(settings: "Settings") -> dict[str, float]:
    """
    Supplier costs: either a worksheet in the same Google spreadsheet as the pipeline
    output (``SUPPLIER_COSTS_SHEET_TAB``), or a local CSV (``SUPPLIER_COSTS_CSV``).
    """
    if settings.supplier_costs_sheet_tab:
        return _load_cost_map_from_google_sheet(settings)
    return _load_cost_map_from_csv(settings.supplier_csv_path)


def _load_cost_map_from_csv(csv_path: Path) -> dict[str, float]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Supplier costs CSV not found: {csv_path}")
    df = pd.read_csv(csv_path)
    return _cost_map_from_dataframe(df, source=str(csv_path))


def _load_cost_map_from_google_sheet(settings: "Settings") -> dict[str, float]:
    from sheets import _authorize, _open_spreadsheet

    tab = (settings.supplier_costs_sheet_tab or "").strip()
    if not tab:
        raise RuntimeError("supplier_costs_sheet_tab is empty")

    client = _authorize(settings)
    sh = _open_spreadsheet(client, settings)
    try:
        ws = sh.worksheet(tab)
    except gspread.exceptions.WorksheetNotFound as exc:
        raise RuntimeError(
            f"Supplier worksheet {tab!r} not found in spreadsheet {sh.title!r}. "
            "Add a tab with header row: Product, Cost"
        ) from exc

    values = ws.get_all_values()
    if not values:
        logger.warning("Supplier sheet %r is empty", tab)
        return {}

    header = [str(c).strip() for c in values[0]]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    return _cost_map_from_dataframe(df, source=f"Google Sheet tab {tab!r}")


def load_cost_map_from_path(csv_path: Path) -> dict[str, float]:
    """Backward-compatible helper for tests and scripts that only have a path."""
    return _load_cost_map_from_csv(csv_path)
