"""Shopify Payments payouts and fee rows for accounting/reporting."""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

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


def _f(val: Any) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def fetch_all_payouts(settings: Settings) -> list[dict[str, Any]]:
    """
    List Shopify Payments payouts.
    Requires Shopify Payments-related scopes/permissions in the app.
    """
    base = (
        f"https://{settings.shopify_store}/admin/api/"
        f"{settings.shopify_api_version}/shopify_payments/payouts.json"
    )
    params: dict[str, Any] = {"limit": 250}
    payouts: list[dict[str, Any]] = []
    url: str | None = base
    headers = {"X-Shopify-Access-Token": get_shopify_access_token(settings)}
    while url:
        response = get_response(
            url,
            settings=settings,
            params=params if url == base else None,
            headers=headers,
        )
        payload = response.json() or {}
        batch = payload.get("payouts") or []
        if isinstance(batch, list):
            payouts.extend([p for p in batch if isinstance(p, dict)])
        url = _next_url_from_link_header(response.headers.get("Link"))
    return payouts


def _fetch_payout_transactions(settings: Settings, payout_id: str) -> list[dict[str, Any]]:
    base = (
        f"https://{settings.shopify_store}/admin/api/"
        f"{settings.shopify_api_version}/shopify_payments/payouts/{payout_id}/transactions.json"
    )
    params: dict[str, Any] = {"limit": 250}
    out: list[dict[str, Any]] = []
    url: str | None = base
    headers = {"X-Shopify-Access-Token": get_shopify_access_token(settings)}
    while url:
        response = get_response(
            url,
            settings=settings,
            params=params if url == base else None,
            headers=headers,
        )
        payload = response.json() or {}
        batch = payload.get("transactions") or []
        if isinstance(batch, list):
            out.extend([t for t in batch if isinstance(t, dict)])
        url = _next_url_from_link_header(response.headers.get("Link"))
    return out


def payout_fee_rows(settings: Settings) -> list[dict[str, Any]]:
    """
    Return transaction-level rows for a dedicated PAYOUTS_FEES sheet.
    """
    payouts = fetch_all_payouts(settings)
    rows: list[dict[str, Any]] = []
    for p in payouts:
        payout_id = str(p.get("id") or "").strip()
        if not payout_id:
            continue
        payout_date = str(p.get("date") or "")[:10]
        status = str(p.get("status") or "")
        currency = str(p.get("currency") or "")
        try:
            transactions = _fetch_payout_transactions(settings, payout_id)
        except Exception as exc:
            logger.warning("Shopify payout %s transactions fetch failed: %s", payout_id, exc)
            continue
        for tx in transactions:
            fee = abs(
                _f(tx.get("fee"))
                or _f(tx.get("fee_amount"))
                or _f(tx.get("processing_fee"))
            )
            rows.append(
                {
                    "Date": payout_date,
                    "Payout_ID": payout_id,
                    "Payout_Status": status,
                    "Currency": currency,
                    "Transaction_ID": str(tx.get("id") or ""),
                    "Transaction_Type": str(tx.get("type") or ""),
                    "Source_Type": str(tx.get("source_type") or ""),
                    "Source_Order_ID": str(tx.get("source_order_id") or tx.get("order_id") or ""),
                    "Gross_Amount": round(_f(tx.get("amount")), 2),
                    "Fee_Amount": round(fee, 2),
                    "Net_Amount": round(_f(tx.get("net")), 2),
                }
            )
    logger.info("Shopify payouts: %s payouts -> %s payout transaction rows", len(payouts), len(rows))
    return rows


def payout_fees_monthly(payouts_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate payout fees by month for BOOKKEEPING merge."""
    if payouts_df.empty or "Date" not in payouts_df.columns or "Fee_Amount" not in payouts_df.columns:
        return pd.DataFrame(columns=["Month", "Payout_Fees_Total"])
    df = payouts_df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    if df.empty:
        return pd.DataFrame(columns=["Month", "Payout_Fees_Total"])
    df["Month"] = df["Date"].dt.to_period("M").astype(str)
    out = (
        df.groupby("Month", as_index=False)["Fee_Amount"]
        .sum()
        .rename(columns={"Fee_Amount": "Payout_Fees_Total"})
    )
    out["Payout_Fees_Total"] = pd.to_numeric(out["Payout_Fees_Total"], errors="coerce").fillna(0.0).round(2)
    return out
