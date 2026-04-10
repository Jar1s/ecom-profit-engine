"""
Optional carrier tracking via 17TRACK API (not Shopify).

Shopify exposes tracking numbers on fulfillments; 17TRACK aggregates many carriers.
Set TRACK17_API_KEY (from https://api.17track.net) to fill ``Carrier_Tracking_Status``.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from config import Settings
from http_retry import post_json_with_retry

logger = logging.getLogger(__name__)

TRACK17_REGISTER = "https://api.17track.net/track/v2/register"
TRACK17_GETTRACKINFO = "https://api.17track.net/track/v2/gettrackinfo"
TRACK17_BATCH_MAX = 40


def extract_tracking_numbers_from_order(order: dict[str, Any]) -> list[str]:
    nums: list[str] = []
    for f in order.get("fulfillments") or []:
        raw = f.get("tracking_numbers") or []
        if not raw and f.get("tracking_number"):
            raw = [f["tracking_number"]]
        for n in raw:
            s = str(n).strip()
            if s and s not in nums:
                nums.append(s)
    return nums


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


def track17_register_and_status(
    settings: Settings,
    api_key: str,
    numbers: list[str],
) -> dict[str, str]:
    """Register + gettrackinfo for up to 40 numbers; map number -> status text."""
    if not numbers:
        return {}
    headers = {"Content-Type": "application/json", "17token": api_key}
    body = [{"number": n, "carrier": 0} for n in numbers]
    out: dict[str, str] = {}

    reg = post_json_with_retry(settings, TRACK17_REGISTER, headers=headers, json_body=body)
    if reg.get("code") != 0:
        logger.warning("17TRACK register: code=%s body=%s", reg.get("code"), reg)

    for item in (reg.get("data") or {}).get("accepted") or []:
        num = item.get("number")
        ps = item.get("package_status")
        if num and ps:
            out[str(num)] = str(ps).strip()

    info = post_json_with_retry(settings, TRACK17_GETTRACKINFO, headers=headers, json_body=body)
    if info.get("code") != 0:
        logger.warning("17TRACK gettrackinfo: code=%s body=%s", info.get("code"), info)
        return out

    for item in (info.get("data") or {}).get("accepted") or []:
        num = item.get("number")
        if not num:
            continue
        s = _status_from_track17_accepted_item(item)
        if s:
            out[str(num)] = s
    return out


def enrich_orders_carrier_tracking(settings: Settings, orders: list[dict[str, Any]]) -> int:
    """
    Set ``order['_carrier_tracking_status']`` from 17TRACK when TRACK17_API_KEY is set.
    Returns number of distinct tracking numbers queried.
    """
    if not settings.track17_api_key or not orders:
        return 0

    pairs: list[tuple[dict[str, Any], str]] = []
    for order in orders:
        nums = extract_tracking_numbers_from_order(order)
        if nums:
            pairs.append((order, nums[0]))

    max_n = settings.track17_max_trackings_per_run
    if max_n > 0 and len(pairs) > max_n:
        logger.warning(
            "17TRACK: capping at %s orders (TRACK17_MAX_TRACKINGS_PER_RUN); increase limit if needed.",
            max_n,
        )
        pairs = pairs[:max_n]

    by_num: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for order, num in pairs:
        by_num[num].append(order)

    unique_numbers = list(by_num.keys())
    status_by_number: dict[str, str] = {}
    for i in range(0, len(unique_numbers), TRACK17_BATCH_MAX):
        chunk = unique_numbers[i : i + TRACK17_BATCH_MAX]
        try:
            part = track17_register_and_status(settings, settings.track17_api_key, chunk)
            status_by_number.update(part)
        except Exception as exc:
            logger.warning("17TRACK batch failed: %s", exc)

    for num, ords in by_num.items():
        st = status_by_number.get(num, "").strip()
        for o in ords:
            o["_carrier_tracking_status"] = st

    if unique_numbers:
        logger.info("17TRACK: queried %s distinct tracking numbers", len(unique_numbers))
    return len(unique_numbers)


def order_tracking_columns(order: dict[str, Any]) -> dict[str, Any]:
    nums = extract_tracking_numbers_from_order(order)
    comps = extract_tracking_companies_from_order(order)
    ext = str(order.get("_carrier_tracking_status") or "").strip()
    return {
        "Tracking_Numbers": ", ".join(nums),
        "Tracking_Companies": ", ".join(comps),
        "Carrier_Tracking_Status": ext,
    }
