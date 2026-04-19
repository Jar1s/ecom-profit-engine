"""Load and validate environment configuration."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

_ROOT = Path(__file__).resolve().parent

# Ak nie je SUPPLIER_COSTS_SHEET_TAB, použije sa táto záložka (automaticky sa vytvorí pri prvom zápise/načítaní).
DEFAULT_SUPPLIER_COSTS_SHEET_TAB = "SUPPLIER_COSTS"

_GOOGLE_SHEET_ID_IN_URL = re.compile(
    r"/spreadsheets/d/([a-zA-Z0-9_-]+)", re.IGNORECASE
)


def _google_sheet_id_from_url(url: str) -> str | None:
    m = _GOOGLE_SHEET_ID_IN_URL.search(url.strip())
    return m.group(1) if m else None


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
    meta_fb_exchange: bool
    ad_account_id: str
    meta_api_version: str
    meta_lookback_days: int
    meta_time_range_since: str | None
    meta_time_range_until: str | None
    meta_campaign_insights: bool
    google_sheet_id: str | None
    google_sheet_name: str | None
    google_creds_path: Path | None
    google_service_account_info: dict[str, Any] | None
    supplier_csv_path: Path
    supplier_costs_sheet_tab: str | None
    http_max_retries: int
    http_backoff_base_seconds: float
    report_currency: str
    usd_per_local: float | None
    meta_spend_in_usd: bool
    sheets_fancy_layout: bool
    meta_purchase_action_types: tuple[str, ...]
    meta_action_attribution_windows: tuple[str, ...] | None
    sheets_conditional_format: bool
    sheets_roas_warn_below: float | None  # None = do not highlight Marketing_ROAS
    daily_summary_usd_primary: bool  # DAILY_SUMMARY: only USD columns + Net_Profit (no duplicate AUD)
    supplier_bill_single_orders_tab: str | None  # BillDetail one-line orders → match by order #; empty = off
    learn_costs_from_orders_sheet: bool  # Median unit cost from last ORDERS_DB when supplier row missing
    item_catalog_sheet_tab: str | None  # ITEM_CATALOG: SKU_Prefix → UnitCost (same price for color variants)
    missing_supplier_costs_tab: str | None  # Report tab: line items with Product_Cost=0; empty = off
    shopify_fulfillment_enrich: bool  # GET /orders/{id} when list omits fulfillments/shipment_status
    shopify_fulfillment_refetch_early: bool  # Also refetch when only label_* / confirmed (extra API calls)
    shopify_graphql_fulfillment_verify: bool  # Admin GraphQL Fulfillment.displayStatus when REST not delivered
    shopify_graphql_verify_max: int  # Max GraphQL order lookups per run; 0 = no cap
    track17_api_key: str | None  # 17TRACK API — carrier status beyond Shopify (optional)
    track17_max_trackings_per_run: int  # Max distinct tracking numbers per pipeline run (0 = no cap)


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


def _meta_purchase_action_types() -> tuple[str, ...]:
    """
    Ordered list: first matching Meta action_type wins (avoids double-counting when
    both `purchase` and `offsite_conversion.fb_pixel_purchase` appear for the same conversions).
    Override: META_PURCHASE_ACTION_TYPES=offsite_conversion.fb_pixel_purchase,purchase
    """
    raw = os.getenv("META_PURCHASE_ACTION_TYPES", "").strip()
    if raw:
        return tuple(x.strip() for x in raw.split(",") if x.strip())
    return (
        "offsite_conversion.fb_pixel_purchase",
        "purchase",
        "omni_purchase",
        "onsite_conversion.purchase",
    )


def _supplier_bill_single_orders_tab() -> str | None:
    s = os.getenv("SUPPLIER_BILL_SINGLE_ORDERS_TAB", "SUPPLIER_BILL_SINGLE_ORDERS").strip()
    return s if s else None


def _item_catalog_sheet_tab() -> str | None:
    s = os.getenv("ITEM_CATALOG_SHEET_TAB", "ITEM_CATALOG").strip()
    return s if s else None


def _missing_supplier_costs_tab() -> str | None:
    s = os.getenv("MISSING_SUPPLIER_COSTS_TAB", "MISSING_SUPPLIER_COSTS").strip()
    return s if s else None


def _sheets_roas_warn_below() -> float | None:
    """
    Marketing_ROAS conditional highlight when below this value (yellow).
    Unset → 1.0; 0 or negative → None (disable ROAS rule).
    """
    raw = os.getenv("SHEETS_ROAS_WARN_BELOW", "").strip()
    if not raw:
        return 1.0
    try:
        v = float(raw)
    except ValueError:
        return 1.0
    return v if v > 0 else None


def _meta_action_attribution_windows() -> tuple[str, ...] | None:
    """
    Optional JSON array, e.g. ["7d_click","1d_view"] to align with Ads Manager columns.
    META_ACTION_ATTRIBUTION_WINDOWS=
    """
    raw = os.getenv("META_ACTION_ATTRIBUTION_WINDOWS", "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "META_ACTION_ATTRIBUTION_WINDOWS must be valid JSON array, e.g. "
            '["7d_click","1d_view"]'
        ) from exc
    if not isinstance(parsed, list):
        raise RuntimeError("META_ACTION_ATTRIBUTION_WINDOWS must be a JSON array")
    out = tuple(str(x).strip() for x in parsed if str(x).strip())
    return out if out else None


def _optional_positive_float_or_none(name: str) -> float | None:
    """Unset or non-positive → None (e.g. USD_PER_LOCAL_UNIT for FX)."""
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return None
    v = float(str(raw).strip())
    return v if v > 0 else None


def _strip_wrapped_quotes(raw: str) -> str:
    """Remove accidental surrounding quotes from pasted env values."""
    s = raw.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        s = s[1:-1].strip()
    return s


def _sanitize_shopify_secret(raw: str) -> str:
    """
    Normalize pasted secrets: quotes, outer trim, and any whitespace/newlines
    (Vercel multiline or bad copy-paste) — Admin API tokens are a single token string.
    """
    s = _strip_wrapped_quotes(raw).strip()
    if not s:
        return ""
    return "".join(s.split())


def _env_bool(name: str, default: bool = True) -> bool:
    """False if env is 0/false/no/off (case-insensitive); True if unset or other."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _normalize_shopify_store(raw: str) -> str:
    """Accept store.myshopify.com or https://store.myshopify.com/ (common paste mistakes)."""
    s = raw.strip()
    if s.startswith("https://"):
        s = s[8:]
    elif s.startswith("http://"):
        s = s[7:]
    s = s.split("/")[0].strip()
    return s.lower()


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

    shopify_token = _sanitize_shopify_secret(os.getenv("SHOPIFY_TOKEN", "")) or None
    shopify_client_id = _sanitize_shopify_secret(os.getenv("SHOPIFY_CLIENT_ID", "")) or None
    shopify_client_secret = _sanitize_shopify_secret(os.getenv("SHOPIFY_CLIENT_SECRET", "")) or None

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

    shopify_store = _normalize_shopify_store(_require("SHOPIFY_STORE"))
    meta_token = _strip_wrapped_quotes(_require("META_TOKEN"))

    google_sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip() or None
    sheet_url = os.getenv("GOOGLE_SHEET_URL", "").strip() or None
    if not google_sheet_id and sheet_url:
        google_sheet_id = _google_sheet_id_from_url(sheet_url)
        if not google_sheet_id:
            raise RuntimeError(
                "GOOGLE_SHEET_URL must contain .../spreadsheets/d/SHEET_ID/... "
                "(paste the browser URL of the spreadsheet)."
            )
    google_sheet_name = os.getenv("GOOGLE_SHEET_NAME", "").strip() or None
    if not google_sheet_id and not google_sheet_name:
        raise RuntimeError(
            "Set GOOGLE_SHEET_NAME (open by title, needs Drive API) and/or "
            "GOOGLE_SHEET_ID or GOOGLE_SHEET_URL (open by id; only Sheets API required)."
        )

    return Settings(
        shopify_store=shopify_store,
        shopify_token=shopify_token,
        shopify_client_id=shopify_client_id,
        shopify_client_secret=shopify_client_secret,
        shopify_api_version=os.getenv("SHOPIFY_API_VERSION", "2024-10").strip(),
        meta_token=meta_token,
        meta_app_id=meta_app_id,
        meta_app_secret=meta_app_secret,
        meta_fb_exchange=_env_bool("META_FB_EXCHANGE", True),
        ad_account_id=_require("AD_ACCOUNT_ID"),
        meta_api_version=os.getenv("META_API_VERSION", "v18.0").strip(),
        meta_lookback_days=_optional_int("META_LOOKBACK_DAYS", 90),
        meta_time_range_since=os.getenv("META_TIME_RANGE_SINCE", "").strip() or None,
        meta_time_range_until=os.getenv("META_TIME_RANGE_UNTIL", "").strip() or None,
        meta_campaign_insights=_env_bool("META_CAMPAIGN_INSIGHTS", True),
        google_sheet_id=google_sheet_id,
        google_sheet_name=google_sheet_name,
        google_creds_path=google_creds_path,
        google_service_account_info=google_info,
        supplier_csv_path=(_ROOT / csv_rel).resolve(),
        supplier_costs_sheet_tab=(
            None
            if _env_bool("SUPPLIER_COSTS_FROM_CSV", False)
            else (
                os.getenv("SUPPLIER_COSTS_SHEET_TAB", "").strip()
                or DEFAULT_SUPPLIER_COSTS_SHEET_TAB
            )
        ),
        http_max_retries=max(1, _optional_int("HTTP_MAX_RETRIES", 5)),
        http_backoff_base_seconds=max(0.1, _optional_float("HTTP_BACKOFF_BASE_SECONDS", 1.5)),
        report_currency=os.getenv("REPORT_CURRENCY", "AUD").strip(),
        usd_per_local=_optional_positive_float_or_none("USD_PER_LOCAL_UNIT"),
        # Meta Graph API returns spend in the ad account currency (often USD). When True,
        # do not multiply Ad_Spend by USD_PER_LOCAL_UNIT for *_USD columns; merge with
        # Shopify daily (AUD) converts Meta USD → AUD via USD_PER_LOCAL_UNIT.
        meta_spend_in_usd=_env_bool("META_SPEND_IN_USD", True),
        sheets_fancy_layout=_env_bool("SHEETS_FANCY_LAYOUT", True),
        meta_purchase_action_types=_meta_purchase_action_types(),
        meta_action_attribution_windows=_meta_action_attribution_windows(),
        sheets_conditional_format=_env_bool("SHEETS_CONDITIONAL_FORMAT", True),
        sheets_roas_warn_below=_sheets_roas_warn_below(),
        daily_summary_usd_primary=_env_bool("DAILY_SUMMARY_USD_PRIMARY", True),
        supplier_bill_single_orders_tab=_supplier_bill_single_orders_tab(),
        learn_costs_from_orders_sheet=_env_bool("LEARN_COSTS_FROM_ORDERS_SHEET", False),
        item_catalog_sheet_tab=_item_catalog_sheet_tab(),
        missing_supplier_costs_tab=_missing_supplier_costs_tab(),
        shopify_fulfillment_enrich=_env_bool("SHOPIFY_FULFILLMENT_ENRICH", True),
        shopify_fulfillment_refetch_early=_env_bool("SHOPIFY_FULFILLMENT_REFETCH_EARLY", False),
        shopify_graphql_fulfillment_verify=_env_bool("SHOPIFY_GRAPHQL_FULFILLMENT_VERIFY", True),
        shopify_graphql_verify_max=_optional_int("SHOPIFY_GRAPHQL_VERIFY_MAX", 500),
        track17_api_key=os.getenv("TRACK17_API_KEY", "").strip() or None,
        # 0 = no cap (query all distinct tracking numbers in the run). Set a positive limit to protect API quota.
        track17_max_trackings_per_run=_optional_int("TRACK17_MAX_TRACKINGS_PER_RUN", 0),
    )
