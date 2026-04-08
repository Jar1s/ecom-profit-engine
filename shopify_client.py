"""Fetch Shopify orders with cursor pagination; expand line items to rows."""

from __future__ import annotations

import logging
import re
from typing import Any

from config import Settings
from http_retry import get_response
from shopify_auth import get_shopify_access_token

logger = logging.getLogger(__name__)

_NEXT_LINK_RE = re.compile(r'<([^>]+)>\s*;\s*rel="next"')


def _next_url_from_link_header(link_header: str | None) -> str | None:
    if not link_header:
        return None
    match = _NEXT_LINK_RE.search(link_header)
    if match:
        return match.group(1).strip()
    return None


def fetch_all_orders(settings: Settings) -> list[dict[str, Any]]:
    """Return all orders from Admin REST API (paginated)."""
    base = (
        f"https://{settings.shopify_store}/admin/api/"
        f"{settings.shopify_api_version}/orders.json"
    )
    params: dict[str, Any] = {"status": "any", "limit": 250}

    orders: list[dict[str, Any]] = []
    url: str | None = base
    page = 0

    while url:
        page += 1
        headers = {"X-Shopify-Access-Token": get_shopify_access_token(settings)}
        if url == base:
            response = get_response(url, settings=settings, params=params, headers=headers)
        else:
            response = get_response(url, settings=settings, params=None, headers=headers)

        payload = response.json()
        batch = payload.get("orders", [])
        orders.extend(batch)
        logger.info("Shopify page %s: fetched %s orders (total so far %s)", page, len(batch), len(orders))

        next_url = _next_url_from_link_header(response.headers.get("Link"))
        url = next_url

    return orders


def orders_to_line_rows(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per line item; Revenue = unit price * quantity (MVP; discounts not allocated)."""
    rows: list[dict[str, Any]] = []
    for order in orders:
        created = order.get("created_at") or ""
        date_str = created[:10] if len(created) >= 10 else ""
        order_id = order.get("id")
        order_name = order.get("name") or ""

        for item in order.get("line_items") or []:
            line_id = item.get("id")
            title = (item.get("name") or item.get("title") or "").strip()
            try:
                qty = int(item.get("quantity") or 0)
            except (TypeError, ValueError):
                qty = 0
            try:
                unit_price = float(item.get("price") or 0)
            except (TypeError, ValueError):
                unit_price = 0.0
            revenue = round(unit_price * qty, 2)

            sku = str(item.get("sku") or "").strip()
            rows.append(
                {
                    "Date": date_str,
                    "Order": order_name,
                    "Order_ID": order_id,
                    "Line_Item_ID": line_id,
                    "Product": title,
                    "SKU": sku,
                    "Quantity": qty,
                    "Revenue": revenue,
                }
            )
    return rows


def fetch_orders_and_line_rows(settings: Settings) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Paginate orders once; return (raw orders, flattened line-item rows)."""
    orders = fetch_all_orders(settings)
    rows = orders_to_line_rows(orders)
    logger.info("Shopify: %s orders -> %s line rows", len(orders), len(rows))
    return orders, rows


def fetch_order_line_rows(settings: Settings) -> list[dict[str, Any]]:
    """High-level: paginate orders and return flattened line-item rows."""
    _, rows = fetch_orders_and_line_rows(settings)
    return rows
