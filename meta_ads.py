"""Fetch Meta Ads insights: account daily spend + campaign daily spend & conversions."""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any

from config import Settings
from http_retry import get_json
from meta_auth import get_meta_access_token

logger = logging.getLogger(__name__)


def _normalize_ad_account_id(raw: str) -> str:
    cleaned = raw.strip()
    if cleaned.startswith("act_"):
        return cleaned[4:]
    return cleaned


def _resolve_time_range(settings: Settings) -> tuple[str, str]:
    if settings.meta_time_range_since and settings.meta_time_range_until:
        return settings.meta_time_range_since, settings.meta_time_range_until
    until_d = date.today()
    since_d = until_d - timedelta(days=max(1, settings.meta_lookback_days) - 1)
    return since_d.isoformat(), until_d.isoformat()


def _raise_if_meta_error(payload: dict[str, Any]) -> None:
    if "error" not in payload:
        return
    err = payload["error"]
    code = err.get("code")
    msg = err.get("message", "Unknown Meta API error")
    sub = f" (code {code})" if code is not None else ""
    if code in (190, 102):
        raise RuntimeError(
            f"Meta API authentication failed{sub}: {msg}. "
            "Update META_TOKEN; with META_APP_ID + META_APP_SECRET the pipeline "
            "refreshes long-lived tokens each run."
        ) from None
    raise RuntimeError(f"Meta API error{sub}: {msg}") from None


def _paginate_insights(
    settings: Settings,
    base_url: str,
    first_params: dict[str, Any],
) -> list[dict[str, Any]]:
    """Collect all `data` rows from paged Graph insights responses."""
    out: list[dict[str, Any]] = []
    url: str | None = base_url
    page = 0
    while url:
        page += 1
        if url == base_url:
            payload = get_json(url, settings=settings, params=first_params)
        else:
            payload = get_json(url, settings=settings, params=None)
        _raise_if_meta_error(payload)
        batch = payload.get("data") or []
        out.extend(batch)
        paging = payload.get("paging") or {}
        url = paging.get("next") if paging.get("next") else None
        logger.info("Meta insights page %s: %s rows (running total %s)", page, len(batch), len(out))
    return out


def _action_list_to_map(items: Any) -> dict[str, float]:
    """Meta sometimes sends duplicate action_type rows; sum values per type."""
    out: dict[str, float] = {}
    for item in items or []:
        if not isinstance(item, dict):
            continue
        at = str(item.get("action_type") or "").strip()
        if not at:
            continue
        try:
            v = float(item.get("value") or 0)
        except (TypeError, ValueError):
            continue
        out[at] = out.get(at, 0.0) + v
    return out


def _pick_purchase_key(
    order: tuple[str, ...], ac: dict[str, float], av: dict[str, float]
) -> str | None:
    """First action_type in priority that appears in actions or action_values (same row in Meta)."""
    for key in order:
        if key in ac or key in av:
            return key
    return None


# First matching type wins (avoids double-counting overlapping Meta action rows).
_ADD_TO_CART_ACTION_TYPES: tuple[str, ...] = (
    "offsite_conversion.fb_pixel_add_to_cart",
    "add_to_cart",
    "omni_add_to_cart",
)
_INITIATE_CHECKOUT_ACTION_TYPES: tuple[str, ...] = (
    "offsite_conversion.fb_pixel_initiate_checkout",
    "initiate_checkout",
    "omni_initiated_checkout",
)


def conversion_count_from_actions(actions: Any, type_order: tuple[str, ...]) -> float:
    """Single canonical conversion count from `actions` (same priority idea as purchases)."""
    ac = _action_list_to_map(actions)
    for key in type_order:
        if key in ac:
            return ac[key]
    return 0.0


def _insight_float(row: dict[str, Any], key: str) -> float:
    try:
        v = row.get(key)
        if v is None or v == "":
            return 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def purchase_metrics_from_insights(
    actions: Any,
    action_values: Any,
    *,
    purchase_action_types: tuple[str, ...],
) -> tuple[float, float]:
    """
    Count and value for **one** canonical purchase action_type.

    Summing every action whose name contains \"purchase\" inflates totals: Meta often returns
    both `purchase` and `offsite_conversion.fb_pixel_purchase` for overlapping conversions.
    Ads Manager uses one primary metric; we take the first matching type from the configured order.
    """
    ac = _action_list_to_map(actions)
    av = _action_list_to_map(action_values)
    key = _pick_purchase_key(purchase_action_types, ac, av)
    if key is None:
        return 0.0, 0.0
    return ac.get(key, 0.0), av.get(key, 0.0)


def fetch_meta_daily_spend(settings: Settings) -> list[dict[str, Any]]:
    """
    Daily spend rows: Date, Ad_Spend (account level).
    """
    act_id = _normalize_ad_account_id(settings.ad_account_id)
    base = f"https://graph.facebook.com/{settings.meta_api_version}/act_{act_id}/insights"
    since, until = _resolve_time_range(settings)
    time_range = json.dumps({"since": since, "until": until})
    access_token = get_meta_access_token(settings)
    first_params: dict[str, Any] = {
        "access_token": access_token,
        "fields": "date_start,spend",
        "time_increment": "1",
        "time_range": time_range,
        "limit": "500",
    }
    data = _paginate_insights(settings, base, first_params)
    rows: list[dict[str, Any]] = []
    for d in data:
        ds = d.get("date_start") or ""
        try:
            spend = float(d.get("spend") or 0)
        except (TypeError, ValueError):
            spend = 0.0
        rows.append({"Date": ds, "Ad_Spend": spend})
    rows = _aggregate_daily_spend_by_date(rows)
    total = sum(float(r.get("Ad_Spend") or 0) for r in rows)
    logger.info(
        "Meta account daily spend: %s days, sum=%.2f (API currency), range %s .. %s",
        len(rows),
        total,
        since,
        until,
    )
    return rows


def _aggregate_daily_spend_by_date(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sum spend per date_start (Graph API can occasionally return duplicate days)."""
    if not rows:
        return rows
    by_date: dict[str, float] = {}
    for r in rows:
        ds = str(r.get("Date") or "").strip()
        if not ds:
            continue
        try:
            v = float(r.get("Ad_Spend") or 0)
        except (TypeError, ValueError):
            v = 0.0
        by_date[ds] = by_date.get(ds, 0.0) + v
    return [{"Date": ds, "Ad_Spend": round(v, 2)} for ds, v in sorted(by_date.items())]


def fetch_meta_campaign_insights(settings: Settings) -> list[dict[str, Any]]:
    """
    Daily rows per campaign: spend, delivery, rates (CPM, CPC, CTR), funnel conversions
    from actions (purchases, add to cart, initiate checkout), purchase value.
    """
    act_id = _normalize_ad_account_id(settings.ad_account_id)
    base = f"https://graph.facebook.com/{settings.meta_api_version}/act_{act_id}/insights"
    since, until = _resolve_time_range(settings)
    time_range = json.dumps({"since": since, "until": until})
    access_token = get_meta_access_token(settings)
    first_params: dict[str, Any] = {
        "access_token": access_token,
        "level": "campaign",
        "fields": (
            "date_start,campaign_id,campaign_name,spend,impressions,clicks,"
            "cpm,cpc,ctr,cost_per_inline_link_click,inline_link_click_ctr,"
            "actions,action_values"
        ),
        "time_increment": "1",
        "time_range": time_range,
        "limit": "500",
    }
    if settings.meta_action_attribution_windows:
        first_params["action_attribution_windows"] = json.dumps(
            list(settings.meta_action_attribution_windows)
        )
    data = _paginate_insights(settings, base, first_params)
    rows: list[dict[str, Any]] = []
    for d in data:
        ds = d.get("date_start") or ""
        cid = d.get("campaign_id") or ""
        cname = str(d.get("campaign_name") or "")
        try:
            spend = float(d.get("spend") or 0)
        except (TypeError, ValueError):
            spend = 0.0
        try:
            impressions = int(float(d.get("impressions") or 0))
        except (TypeError, ValueError):
            impressions = 0
        try:
            clicks = int(float(d.get("clicks") or 0))
        except (TypeError, ValueError):
            clicks = 0
        purchases, purchase_value = purchase_metrics_from_insights(
            d.get("actions"),
            d.get("action_values"),
            purchase_action_types=settings.meta_purchase_action_types,
        )
        adds_cart = conversion_count_from_actions(d.get("actions"), _ADD_TO_CART_ACTION_TYPES)
        checkouts = conversion_count_from_actions(
            d.get("actions"), _INITIATE_CHECKOUT_ACTION_TYPES
        )
        rows.append(
            {
                "Date": ds,
                "Campaign_ID": str(cid),
                "Campaign_Name": cname,
                "Ad_Spend": round(spend, 2),
                "Impressions": impressions,
                "Clicks": clicks,
                "CPM": round(_insight_float(d, "cpm"), 4),
                "CPC_All": round(_insight_float(d, "cpc"), 4),
                "CPC_Link": round(_insight_float(d, "cost_per_inline_link_click"), 4),
                "CTR_All_pct": round(_insight_float(d, "ctr"), 4),
                "CTR_Link_pct": round(_insight_float(d, "inline_link_click_ctr"), 4),
                "Adds_to_Cart": round(adds_cart, 2),
                "Checkouts_Initiated": round(checkouts, 2),
                "Purchases": round(purchases, 2),
                "Purchase_Value": round(purchase_value, 2),
            }
        )
    logger.info(
        "Meta: %s campaign×day insight rows for %s .. %s",
        len(rows),
        since,
        until,
    )
    return rows
