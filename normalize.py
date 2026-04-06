"""Normalize product titles for supplier cost matching."""

from __future__ import annotations

import re

_MULTI_SPACE = re.compile(r"\s+")


def normalize_product_name(name: str) -> str:
    """Trim, collapse whitespace, casefold for case-insensitive lookup."""
    if not name:
        return ""
    s = name.strip()
    s = _MULTI_SPACE.sub(" ", s)
    return s.casefold()
