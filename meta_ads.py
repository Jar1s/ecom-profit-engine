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


def _purchase_metrics(actions: Any, action_values: Any) -> tuple[float, float]:
    """
    Sum Meta conversion counts and values for action types whose name contains 'purchase'.
    Covers purchase, omni_purchase, offsite_conversion.fb_pixel_purchase, etc.
    """
    purchase_count = 0.0
    purchase_value = 0.0
    for item in actions or []:
        if not isinstance(item, dict):
            continue
        at = str(item.get("action_type") or "")
        if "purchase" not in at.lower():
            continue
        try:
            purchase_count += float(item.get("value") or 0)
        except (TypeError, ValueError):
            pass
    for item in action_values or []:
        if not isinstance(item, dict):
            continue
        at = str(item.get("action_type") or "")
        if "purchase" not in at.lower():
            continue
        try:
            purchase_value += float(item.get("value") or 0)
        except (TypeError, ValueError):
            pass
    return purchase_count, purchase_value


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
    logger.info("Meta: %s account daily spend rows for %s .. %s", len(rows), since, until)
    return rows


def fetch_meta_campaign_insights(settings: Settings) -> list[dict[str, Any]]:
    """
    Daily rows per campaign: Date, Campaign_ID, Campaign_Name, Ad_Spend, Impressions,
    Clicks, Purchases (conversion count from actions), Purchase_Value (from action_values).
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
            "date_start,campaign_id,campaign_name,spend,impressions,clicks,actions,action_values"
        ),
        "time_increment": "1",
        "time_range": time_range,
        "limit": "500",
    }
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
        purchases, purchase_value = _purchase_metrics(d.get("actions"), d.get("action_values"))
        rows.append(
            {
                "Date": ds,
                "Campaign_ID": str(cid),
                "Campaign_Name": cname,
                "Ad_Spend": round(spend, 2),
                "Impressions": impressions,
                "Clicks": clicks,
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
