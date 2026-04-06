"""Load and validate environment configuration."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

_ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Settings:
    shopify_store: str
    shopify_token: str | None
    shopify_client_id: str | None
    shopify_client_secret: str | None
    shopify_api_version: str
    meta_token: str
    meta_app_id: str | None
    meta_app_secret: str | None
    ad_account_id: str
    meta_api_version: str
    meta_lookback_days: int
    meta_time_range_since: str | None
    meta_time_range_until: str | None
    google_sheet_name: str
    google_creds_path: Path | None
    google_service_account_info: dict[str, Any] | None
    supplier_csv_path: Path
    http_max_retries: int
    http_backoff_base_seconds: float


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value or not value.strip():
        raise RuntimeError(f"Missing or empty required environment variable: {name}")
    return value.strip()


def _optional_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _optional_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def load_settings() -> Settings:
    csv_rel = os.getenv("SUPPLIER_COSTS_CSV", "data/supplier_costs.csv").strip()

    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    google_creds_path: Path | None
    google_info: dict[str, Any] | None
    if creds_json:
        try:
            parsed = json.loads(creds_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError("GOOGLE_CREDENTIALS_JSON is not valid JSON") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("GOOGLE_CREDENTIALS_JSON must be a JSON object")
        google_creds_path = None
        google_info = parsed
    else:
        creds = os.getenv("GOOGLE_CREDS", "creds.json").strip()
        google_creds_path = (_ROOT / creds).resolve()
        google_info = None

    shopify_token = os.getenv("SHOPIFY_TOKEN", "").strip() or None
    shopify_client_id = os.getenv("SHOPIFY_CLIENT_ID", "").strip() or None
    shopify_client_secret = os.getenv("SHOPIFY_CLIENT_SECRET", "").strip() or None

    if shopify_token and (shopify_client_id or shopify_client_secret):
        raise RuntimeError(
            "Set either SHOPIFY_TOKEN (static custom app) or "
            "SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET (Dev Dashboard), not both."
        )
    if not shopify_token and not (shopify_client_id and shopify_client_secret):
        raise RuntimeError(
            "Shopify: set SHOPIFY_TOKEN or both SHOPIFY_CLIENT_ID and SHOPIFY_CLIENT_SECRET."
        )

    meta_app_id = os.getenv("META_APP_ID", "").strip() or None
    meta_app_secret = os.getenv("META_APP_SECRET", "").strip() or None
    if meta_app_id and not meta_app_secret:
        raise RuntimeError("META_APP_SECRET is required when META_APP_ID is set.")
    if meta_app_secret and not meta_app_id:
        raise RuntimeError("META_APP_ID is required when META_APP_SECRET is set.")

    return Settings(
        shopify_store=_require("SHOPIFY_STORE"),
        shopify_token=shopify_token,
        shopify_client_id=shopify_client_id,
        shopify_client_secret=shopify_client_secret,
        shopify_api_version=os.getenv("SHOPIFY_API_VERSION", "2024-10").strip(),
        meta_token=_require("META_TOKEN"),
        meta_app_id=meta_app_id,
        meta_app_secret=meta_app_secret,
        ad_account_id=_require("AD_ACCOUNT_ID"),
        meta_api_version=os.getenv("META_API_VERSION", "v18.0").strip(),
        meta_lookback_days=_optional_int("META_LOOKBACK_DAYS", 90),
        meta_time_range_since=os.getenv("META_TIME_RANGE_SINCE", "").strip() or None,
        meta_time_range_until=os.getenv("META_TIME_RANGE_UNTIL", "").strip() or None,
        google_sheet_name=_require("GOOGLE_SHEET_NAME"),
        google_creds_path=google_creds_path,
        google_service_account_info=google_info,
        supplier_csv_path=(_ROOT / csv_rel).resolve(),
        http_max_retries=max(1, _optional_int("HTTP_MAX_RETRIES", 5)),
        http_backoff_base_seconds=max(0.1, _optional_float("HTTP_BACKOFF_BASE_SECONDS", 1.5)),
    )
