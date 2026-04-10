"""
Optional carrier tracking via 17TRACK API (not Shopify).

Shopify exposes tracking numbers on fulfillments; 17TRACK aggregates many carriers.
Set TRACK17_API_KEY (from https://api.17track.net) to fill ``Carrier_Tracking_Status``.

Reliability notes:
- We map ``tracking_company`` strings from Shopify to 17TRACK carrier IDs (auto-detect ``carrier: 0``
  often yields NotFound).
- All tracking numbers on an order are queried (not only the first).
- When 17TRACK is empty / NotFound, the sheet cell falls back to Shopify rollup (``Shopify: …``).
"""

from __future__ import annotations

import logging
import re
from typing import Any

from config import Settings
from http_retry import post_json_with_retry

logger = logging.getLogger(__name__)

TRACK17_REGISTER = "https://api.17track.net/track/v2/register"
TRACK17_GETTRACKINFO = "https://api.17track.net/track/v2/gettrackinfo"
TRACK17_BATCH_MAX = 40

# Substring match on normalized ``tracking_company`` (longest alias wins). IDs from 17TRACK carrier list.
_RAW_COMPANY_ALIASES: list[tuple[str, int]] = [
    ("dhl express", 100001),
    ("dhl ecommerce asia", 7048),
    ("dhl ecommerce us", 7047),
    ("dhl ecommerce", 7047),
    ("dhl parcel (nl)", 100047),
    ("dhl parcel nl", 100047),
    ("dhl paket", 7041),
    ("dhl", 100001),
    ("amazon shipping", 100308),
    ("amazon logistics", 100308),
    ("amazon", 100308),
    ("ups mail innovations", 100398),
    ("ups", 100002),
    ("fedex", 100003),
    ("usps", 21051),
    ("gls italy", 100024),
    ("gls croatia", 100207),
    ("gls spain", 100189),
    ("gls", 100005),
    ("dpd uk", 100010),
    ("dpd france", 100072),
    ("dpd fr", 100072),
    ("dpd germany", 100007),
    ("dpd de", 100007),
    ("dpd", 100007),
    ("zasilkovna", 100419),
    ("zásilkovna", 100419),
    ("packeta", 100132),
    ("slovakia post", 19141),
    ("slovak parcel service", 100407),
    ("slovak post", 19141),
    ("slovak", 19141),
    ("royal mail", 11031),
    ("evri", 100331),
    ("hermes", 100018),
    ("inpost", 100043),
    ("colissimo", 6051),
    ("la poste", 6051),
    ("czech post", 3221),
    ("magyar posta", 8051),
    ("postnord sweden", 19241),
    ("postnord", 19241),
    ("bring", 100423),
    ("db schenker", 100206),
    ("geodis", 100356),
    ("tnt", 100004),
    ("aramex", 100006),
    ("canada post", 3041),
    ("australia post", 1151),
    ("canada", 3041),
    ("australian", 1151),
    ("correos", 100303),
    ("tnt france", 100241),
    ("brt", 100026),
    ("ppl cz", 100176),
    ("ppl", 100176),
]
# Longest alias first so "dhl express" wins over "dhl".
_TRACKING_COMPANY_ALIASES = sorted(_RAW_COMPANY_ALIASES, key=lambda x: -len(x[0]))


def _normalize_company_name(company: str) -> str:
    s = company.strip().lower()
    s = s.replace("’", "'").replace("`", "'")
    s = re.sub(r"\s+", " ", s)
    return s


def resolve_17track_carrier(company: str) -> int:
    """Map Shopify ``tracking_company`` to 17TRACK carrier key, or 0 for auto-detect."""
    if not company or not company.strip():
        return 0
    n = _normalize_company_name(company)
    for alias, key in _TRACKING_COMPANY_ALIASES:
        if alias in n:
            return key
    return 0


def extract_tracking_with_carrier(order: dict[str, Any]) -> list[tuple[str, str]]:
    """(tracking_number, tracking_company) per fulfillment, deduped by number (first wins)."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for f in order.get("fulfillments") or []:
        raw = f.get("tracking_numbers") or []
        if not raw and f.get("tracking_number"):
            raw = [f["tracking_number"]]
        company = (f.get("tracking_company") or "").strip()
        for n in raw:
            s = str(n).strip()
            if s and s not in seen:
                seen.add(s)
                out.append((s, company))
    return out


def extract_tracking_numbers_from_order(order: dict[str, Any]) -> list[str]:
    return [n for n, _ in extract_tracking_with_carrier(order)]


def extract_tracking_companies_from_order(order: dict[str, Any]) -> list[str]:
    companies: list[str] = []
    for f in order.get("fulfillments") or []:
        c = (f.get("tracking_company") or "").strip()
        if c and c not in companies:
            companies.append(c)
    return companies


def _status_line_from_track_info(track_info: dict[str, Any]) -> str:
    ls = track_info.get("latest_status") or {}
    le = track_info.get("latest_event") or {}
    desc = le.get("description")
    if desc:
        return str(desc).strip()
    sub = ls.get("sub_status_descr")
    if sub:
        return str(sub).strip()
    for key in ("status", "sub_status"):
        v = ls.get(key)
        if v:
            return str(v).strip()
    return ""


def _status_from_track17_accepted_item(item: dict[str, Any]) -> str:
    ti = item.get("track_info") or {}
    line = _status_line_from_track_info(ti)
    if line:
        return line
    ps = item.get("package_status")
    if ps:
        return str(ps).strip()
    return ""


def _log_track17_rejected(endpoint: str, payload: dict[str, Any]) -> None:
    data = payload.get("data") or {}
    rejected = data.get("rejected") or []
    if not rejected:
        return
    for item in rejected[:15]:
        logger.info("17TRACK %s rejected: %s", endpoint, item)
    if len(rejected) > 15:
        logger.info("17TRACK %s: … and %s more rejected", endpoint, len(rejected) - 15)


def track17_register_and_status(
    settings: Settings,
    api_key: str,
    numbers: list[str],
    num_to_company: dict[str, str],
) -> dict[str, str]:
    """Register + gettrackinfo for up to 40 numbers; map number -> status text."""
    if not numbers:
        return {}
    headers = {"Content-Type": "application/json", "17token": api_key}
    body = [
        {"number": n, "carrier": resolve_17track_carrier(num_to_company.get(n, ""))}
        for n in numbers
    ]
    out: dict[str, str] = {}

    reg = post_json_with_retry(settings, TRACK17_REGISTER, headers=headers, json_body=body)
    if reg.get("code") != 0:
        logger.warning("17TRACK register: code=%s body=%s", reg.get("code"), reg)
    _log_track17_rejected("register", reg)

    for item in (reg.get("data") or {}).get("accepted") or []:
        num = item.get("number")
        ps = item.get("package_status")
        if num and ps:
            out[str(num)] = str(ps).strip()

    info = post_json_with_retry(settings, TRACK17_GETTRACKINFO, headers=headers, json_body=body)
    if info.get("code") != 0:
        logger.warning("17TRACK gettrackinfo: code=%s body=%s", info.get("code"), info)
        return out
    _log_track17_rejected("gettrackinfo", info)

    for item in (info.get("data") or {}).get("accepted") or []:
        num = item.get("number")
        if not num:
            continue
        s = _status_from_track17_accepted_item(item)
        if s:
            out[str(num)] = s
    return out


_CARRIER_FAILURE_TOKENS = frozenset(
    {
        "",
        "notfound",
        "not found",
        "unknown",
        "null",
        "tracking number format error",
    }
)


def _pick_best_carrier_status(statuses: list[str]) -> str:
    """Prefer first non-failure status when an order has multiple tracking numbers."""
    if not statuses:
        return ""
    for s in statuses:
        t = s.strip()
        if t and t.lower() not in _CARRIER_FAILURE_TOKENS:
            return t
    for s in statuses:
        if s and s.strip():
            return s.strip()
    return ""


def enrich_orders_carrier_tracking(settings: Settings, orders: list[dict[str, Any]]) -> int:
    """
    Set ``order['_carrier_tracking_status']`` from 17TRACK when TRACK17_API_KEY is set.
    Returns number of distinct tracking numbers queried.
    """
    if not settings.track17_api_key or not orders:
        return 0

    order_num_pairs: list[tuple[dict[str, Any], str]] = []
    num_to_company: dict[str, str] = {}

    for order in orders:
        for num, company in extract_tracking_with_carrier(order):
            order_num_pairs.append((order, num))
            if num not in num_to_company:
                num_to_company[num] = company

    unique_numbers = list(dict.fromkeys(num for _, num in order_num_pairs))

    max_n = settings.track17_max_trackings_per_run
    if max_n > 0 and len(unique_numbers) > max_n:
        logger.warning(
            "17TRACK: capping at %s distinct numbers (TRACK17_MAX_TRACKINGS_PER_RUN); increase limit if needed.",
            max_n,
        )
        allowed = set(unique_numbers[:max_n])
        unique_numbers = unique_numbers[:max_n]
        order_num_pairs = [(o, n) for o, n in order_num_pairs if n in allowed]

    status_by_number: dict[str, str] = {}
    for i in range(0, len(unique_numbers), TRACK17_BATCH_MAX):
        chunk = unique_numbers[i : i + TRACK17_BATCH_MAX]
        try:
            part = track17_register_and_status(
                settings, settings.track17_api_key, chunk, num_to_company
            )
            status_by_number.update(part)
        except Exception as exc:
            logger.warning("17TRACK batch failed: %s", exc)

    for order in orders:
        nums = extract_tracking_numbers_from_order(order)
        statuses = [status_by_number.get(n, "").strip() for n in nums]
        order["_carrier_tracking_status"] = _pick_best_carrier_status(statuses)

    if unique_numbers:
        mapped = sum(1 for n in unique_numbers if resolve_17track_carrier(num_to_company.get(n, "")) > 0)
        logger.info(
            "17TRACK: queried %s distinct tracking numbers (%s with mapped carrier, %s auto-detect)",
            len(unique_numbers),
            mapped,
            len(unique_numbers) - mapped,
        )
    return len(unique_numbers)


def _carrier_needs_shopify_fallback(carrier: str) -> bool:
    t = carrier.strip().lower()
    if not t:
        return True
    if t in _CARRIER_FAILURE_TOKENS:
        return True
    if "not found" in t or t == "notfound":
        return True
    return False


def order_tracking_columns(
    order: dict[str, Any],
    *,
    ship_cols: dict[str, Any] | None = None,
) -> dict[str, Any]:
    nums = extract_tracking_numbers_from_order(order)
    comps = extract_tracking_companies_from_order(order)
    ext = str(order.get("_carrier_tracking_status") or "").strip()

    if ship_cols is not None and _carrier_needs_shopify_fallback(ext):
        fb = (ship_cols.get("Delivery_Status") or "").strip()
        if fb:
            ext = f"Shopify: {fb}"

    return {
        "Tracking_Numbers": ", ".join(nums),
        "Tracking_Companies": ", ".join(comps),
        "Carrier_Tracking_Status": ext,
    }
