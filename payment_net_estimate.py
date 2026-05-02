"""Optional Payment_Net_Estimate when Shopify payout ledger has no net yet for an order."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from config import Settings

logger = logging.getLogger(__name__)


def classify_payment_bucket(gateway_names: str) -> str:
    """
    Map ``payment_gateway_names`` join string to a fee bucket.
    MVP: paypal | shopify_payments | other (no per-card brand without transaction enrich).

    Shopify often omits the literal ``shopify_payments`` string and sends ``credit_card``,
    ``shop_pay``, ``apple_pay``, ``google_pay``, or bare ``shopify`` for Shopify Payments
    checkout — those map to ``shopify_payments`` so PAYMENT_NET_ESTIMATE_FEES_JSON can
    use one rate for the store's default card/wallet flow.
    """
    s = (gateway_names or "").lower()
    if "paypal" in s:
        return "paypal"
    if (
        "shopify_payments" in s
        or "shopify payments" in s
        or ("shopify" in s and "payment" in s)
    ):
        return "shopify_payments"
    tokens = {t.strip() for t in s.replace(";", ",").split(",") if t.strip()}
    if "shopify" in tokens:
        return "shopify_payments"
    for needle in (
        "credit_card",
        "shop_pay",
        "shop_pay_installments",
        "apple_pay",
        "google_pay",
        "android_pay",
    ):
        if needle in s:
            return "shopify_payments"
    return "other"


def estimate_net_from_revenue(revenue: float, bucket: str, fees_by_bucket: dict[str, dict[str, float]]) -> float | None:
    """``net ≈ max(0, revenue * (1 - pct) - fixed)`` using bucket config or ``other`` fallback."""
    cfg = fees_by_bucket.get(bucket) or fees_by_bucket.get("other")
    if not cfg:
        return None
    try:
        pct = float(cfg.get("pct", 0.0))
        fixed = float(cfg.get("fixed", 0.0))
    except (TypeError, ValueError):
        return None
    pct = max(0.0, min(1.0, pct))
    fixed = max(0.0, fixed)
    try:
        rev = float(revenue)
    except (TypeError, ValueError):
        rev = 0.0
    net = max(0.0, rev * (1.0 - pct) - fixed)
    return round(net, 2)


def apply_payment_net_estimate(df: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """
    Set ``Payment_Net_Estimate`` only when ``PAYMENT_NET_ESTIMATE`` is on and fees JSON is valid.
    If ``Payment_Net`` > 0 (ledger), estimate is cleared to NaN (empty in Sheets / null in JSON).
    """
    if df.empty:
        return df
    out = df.copy()
    if not settings.payment_net_estimate:
        return out.drop(columns=["Payment_Net_Estimate"], errors="ignore")

    fees = settings.payment_net_estimate_fees
    if not fees:
        logger.warning(
            "PAYMENT_NET_ESTIMATE=1 but PAYMENT_NET_ESTIMATE_FEES_JSON missing or invalid; "
            "Payment_Net_Estimate not filled"
        )
        out["Payment_Net_Estimate"] = np.nan
        return out

    if "Payment_Net" not in out.columns:
        out["Payment_Net"] = 0.0
    gateway = (
        out["Payment_Gateway_Names"].fillna("").astype(str)
        if "Payment_Gateway_Names" in out.columns
        else pd.Series("", index=out.index, dtype=str)
    )
    rev = pd.to_numeric(out.get("Revenue", 0.0), errors="coerce").fillna(0.0)
    ledger = pd.to_numeric(out["Payment_Net"], errors="coerce").fillna(0.0)

    estimates: list[float] = []
    missing_fee_rule = 0
    for i in out.index:
        if float(ledger.loc[i]) > 0.0:
            estimates.append(float("nan"))
            continue
        bucket = classify_payment_bucket(str(gateway.loc[i]))
        est = estimate_net_from_revenue(float(rev.loc[i]), bucket, fees)
        if est is None and float(rev.loc[i]) > 0.0:
            missing_fee_rule += 1
        estimates.append(float("nan") if est is None else float(est))

    if missing_fee_rule:
        logger.warning(
            "Payment_Net_Estimate: %s line item(s) have Revenue>0 and Payment_Net=0 but no "
            "matching fee rule (bucket has no entry and no `other` in PAYMENT_NET_ESTIMATE_FEES_JSON). "
            "Add keys e.g. paypal, shopify_payments, other.",
            missing_fee_rule,
        )

    out["Payment_Net_Estimate"] = pd.Series(estimates, index=out.index, dtype=float)
    return out
