#!/usr/bin/env python3
"""Smoke-check: one in-memory pipeline run (no Sheet writes). Row counts only."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import load_settings
from costs import load_cost_maps
from pipeline import _run_pipeline


def main() -> int:
    settings = load_settings()
    cost_maps = load_cost_maps(settings)
    artifacts = _run_pipeline(settings, cost_maps)
    print(
        "OK",
        "orders_df",
        len(artifacts.orders_df),
        "order_level",
        len(artifacts.order_df),
        "meta",
        len(artifacts.meta_df),
        "daily",
        len(artifacts.daily_final),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
