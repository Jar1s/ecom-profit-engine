"""Fetch Shopify orders with cursor pagination; expand line items to rows."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Any

from config import Settings
from http_retry import get_response
from shopify_auth import get_shopify_access_token

logger = logging.getLogger(__name__)

_NEXT_LINK_RE = re.compile(r'<([^>]+)>\s*;\s*rel="next"')

# Shopify Fulfillment.shipment_status — pick the most advanced across fulfillments.
_SHIPMENT_PRIORITY: dict[str, int] = {
    "label_printed": 10,
    "label_purchased": 20,
    "confirmed": 30,
    "in_transit": 40,
    "out_for_delivery": 50,
    "attempted_delivery": 55,
    "delivered": 100,
    "failure": 5,
    "canceled": 0,
}


def _fulfillment_status_str(order: dict[str, Any]) -> str:
    fs = order.get("fulfillment_status")
    if fs is None:
        return "unfulfilled"
    return str(fs).strip().lower()


def _best_shipment_status(fulfillments: list[dict[str, Any]]) -> str:
    best: str | None = None
    best_p = -1
    for f in fulfillments:
        ss = f.get("shipment_status")
        if not ss:
            continue
        s = str(ss).strip().lower()
        p = _SHIPMENT_PRIORITY.get(s, 0)
        if p > best_p:
            best_p = p
            best = s
    return best or ""


def _parse_iso_date(value: object) -> date | None:
    if value is None:
        return None
    s = str(value).strip()
    if len(s) < 10:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def _first_ship_date(fulfillments: list[dict[str, Any]]) -> date | None:
    """Earliest fulfillment created_at (when the order was first marked shipped)."""
    best: date | None = None
    for f in fulfillments:
        d = _parse_iso_date(f.get("created_at"))
        if d is not None and (best is None or d < best):
            best = d
    return best


def _days_in_transit(
    delivery_status: str,
    ship_date: date | None,
    *,
    today: date,
) -> int | None:
    """
    Calendar days since first ship date while the package is still „active“.
    Delivered → 0 (nothing in flight). Unfulfilled / cancelled → no value.
    """
    if delivery_status == "Delivered":
        return 0
    if delivery_status in ("Cancelled", "Unfulfilled"):
        return None
    if ship_date is None:
        return None
    return max(0, (today - ship_date).days)


def _delivery_status_label(order: dict[str, Any], shipment_status: str) -> str:
    """
    Single rollup for reporting: Unfulfilled / Partial / Shipped / In transit / …
    Uses order fulfillment_status + carrier shipment_status from fulfillments.
    """
    if order.get("cancelled_at"):
        return "Cancelled"
    fs = _fulfillment_status_str(order)
    ss = (shipment_status or "").strip().lower()

    if fs == "unfulfilled":
        return "Unfulfilled"
    if fs == "partial":
        return "Partial"
    if fs == "restocked":
        return "Restocked"
    if fs != "fulfilled":
        return fs.replace("_", " ").title() if fs else "Unknown"

    if not ss:
        return "Shipped"

    if ss in ("label_printed", "label_purchased", "confirmed"):
        return "Shipped"
    if ss == "in_transit":
        return "In transit"
    if ss == "out_for_delivery":
        return "Out for delivery"
    if ss == "delivered":
        return "Delivered"
    if ss == "attempted_delivery":
        return "Delivery attempted"
    if ss == "failure":
        return "Delivery failed"
    if ss == "canceled":
        return "Shipment cancelled"

    return ss.replace("_", " ").title()


def order_shipping_columns(order: dict[str, Any], *, today: date | None = None) -> dict[str, Any]:
    """Fulfillment + carrier status + ship date and days in transit (repeat on each line-item row)."""
    fulfillments = list(order.get("fulfillments") or [])
    ship = _best_shipment_status(fulfillments)
    delivery = _delivery_status_label(order, ship)
    ship_date = _first_ship_date(fulfillments)
    t = today if today is not None else datetime.now(timezone.utc).date()
    days = _days_in_transit(delivery, ship_date, today=t)
    return {
        "Fulfillment_Status": _fulfillment_status_str(order),
        "Shipment_Status": ship,
        "Delivery_Status": delivery,
        "Shipped_Date": ship_date.isoformat() if ship_date else "",
        "Days_In_Transit": days if days is not None else "",
    }


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

        ship_cols = order_shipping_columns(order)
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
                    **ship_cols,
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
