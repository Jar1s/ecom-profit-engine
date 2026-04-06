"""Shopify Admin API access: static token or Dev Dashboard client credentials (cached)."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from config import Settings

logger = logging.getLogger(__name__)

# Cache for client-credentials tokens (per process, ~24h lifetime)
_cc_token: str | None = None
_cc_expires_at: float = 0.0


def get_shopify_access_token(settings: Settings) -> str:
    """
    Return X-Shopify-Access-Token value.
    Either SHOPIFY_TOKEN (static custom app) or client credentials grant (Dev Dashboard).
    """
    if settings.shopify_token:
        return settings.shopify_token
    if settings.shopify_client_id and settings.shopify_client_secret:
        return _get_client_credentials_token(settings)
    raise RuntimeError("Shopify auth: internal configuration error")


def _get_client_credentials_token(settings: Settings) -> str:
    global _cc_token, _cc_expires_at
    now = time.time()
    if _cc_token and now < _cc_expires_at - 60:
        return _cc_token

    shop = settings.shopify_store.strip()
    if shop.startswith("https://"):
        shop = shop.replace("https://", "").split("/")[0]
    url = f"https://{shop}/admin/oauth/access_token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": settings.shopify_client_id,
        "client_secret": settings.shopify_client_secret,
    }
    r = requests.post(
        url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=60,
    )
    if not r.ok:
        try:
            body = r.json()
            err = body.get("error_description") or body.get("error") or r.text
        except ValueError:
            err = r.text
        raise RuntimeError(
            f"Shopify client credentials failed ({r.status_code}): {err}. "
            "Ensure the app is installed on this shop and scopes match."
        ) from None

    data: dict[str, Any] = r.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("Shopify token response missing access_token")
    expires_in = int(data.get("expires_in", 86399))
    _cc_token = str(token)
    _cc_expires_at = now + expires_in
    logger.info("Shopify: obtained client-credentials token (expires in %s s)", expires_in)
    return _cc_token
