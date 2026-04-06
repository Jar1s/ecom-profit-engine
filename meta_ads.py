"""Fetch daily Meta Ads spend (Marketing API insights)."""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any

from config import Settings
from http_retry import get_json

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


def fetch_meta_daily_spend(settings: Settings) -> list[dict[str, Any]]:
    """
    Daily spend rows: Date, Ad_Spend.
    Handles Graph API paging and surfaces auth errors clearly.
    """
    act_id = _normalize_ad_account_id(settings.ad_account_id)
    base = f"https://graph.facebook.com/{settings.meta_api_version}/act_{act_id}/insights"

    since, until = _resolve_time_range(settings)
    time_range = json.dumps({"since": since, "until": until})

    rows: list[dict[str, Any]] = []
    url: str | None = base
    params: dict[str, Any] | None = None
    page = 0

    while url:
        page += 1
        if url == base:
            params = {
                "access_token": settings.meta_token,
                "fields": "date_start,spend",
                "time_increment": "1",
                "time_range": time_range,
                "limit": "500",
            }
            payload = get_json(url, settings=settings, params=params)
        else:
            payload = get_json(url, settings=settings, params=None)

        if "error" in payload:
            err = payload["error"]
            code = err.get("code")
            msg = err.get("message", "Unknown Meta API error")
            sub = f" (code {code})" if code is not None else ""
            if code in (190, 102):
                raise RuntimeError(
                    f"Meta API authentication failed{sub}: {msg}. "
                    "Refresh META_TOKEN (long-lived system user token recommended)."
                ) from None
            raise RuntimeError(f"Meta API error{sub}: {msg}") from None

        data = payload.get("data") or []
        for d in data:
            ds = d.get("date_start") or ""
            try:
                spend = float(d.get("spend") or 0)
            except (TypeError, ValueError):
                spend = 0.0
            rows.append({"Date": ds, "Ad_Spend": spend})

        paging = payload.get("paging") or {}
        next_url = paging.get("next")
        url = next_url if next_url else None
        logger.info("Meta insights page %s: %s days (total rows %s)", page, len(data), len(rows))

    logger.info("Meta: %s daily spend rows for %s .. %s", len(rows), since, until)
    return rows
