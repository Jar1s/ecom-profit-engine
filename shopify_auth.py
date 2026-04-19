"""Shopify Admin API access: static token or Dev Dashboard client credentials (cached)."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

import requests

from config import Settings

logger = logging.getLogger(__name__)

_HTML_OAUTH_ERR = re.compile(r"Oauth error\s+(\w+):\s*([^<]+)", re.IGNORECASE)

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


def log_shopify_auth_once(settings: Settings, token: str) -> None:
    """Safe fingerprint for Vercel logs (no full token). Call once per pipeline run."""
    mode = "SHOPIFY_TOKEN" if settings.shopify_token else "client_credentials"
    prefix = token[:10] if len(token) >= 10 else token
    logger.info(
        "Shopify auth: store=%s mode=%s token_len=%s token_prefix=%s",
        settings.shopify_store,
        mode,
        len(token),
        prefix,
    )


def log_shopify_auth_config(settings: Settings) -> None:
    """Log sanitized auth configuration before the first Shopify request."""
    mode = "SHOPIFY_TOKEN" if settings.shopify_token else "client_credentials"
    static_token = settings.shopify_token or ""
    logger.info(
        "Shopify auth config: store=%s mode=%s static_token_present=%s "
        "static_token_len=%s static_token_prefix=%s client_id_present=%s "
        "client_secret_present=%s",
        settings.shopify_store,
        mode,
        bool(static_token),
        len(static_token),
        static_token[:10] if static_token else "",
        bool(settings.shopify_client_id),
        bool(settings.shopify_client_secret),
    )


def shopify_401_diagnostic_hint(settings: Settings, token: str) -> str:
    """Actionable, sanitized context for Shopify 401 responses."""
    mode = "SHOPIFY_TOKEN" if settings.shopify_token else "client_credentials"
    prefix = token[:10] if len(token) >= 10 else token
    base = (
        f" Shopify auth diagnostic: store={settings.shopify_store} mode={mode} "
        f"token_len={len(token)} token_prefix={prefix}."
    )
    if settings.shopify_token:
        hint = (
            " Most likely: Production Vercel env has an old/revoked SHOPIFY_TOKEN, "
            "the token belongs to a different shop than SHOPIFY_STORE, or a non-Admin "
            "secret was pasted instead of the Custom App Admin API token."
        )
        if not token.startswith("shpat_"):
            hint += " Token prefix is not shpat_; verify you pasted the Admin API access token."
        hint += " Re-copy the token from Shopify Admin → custom app → API credentials, update Production env, and redeploy."
        return base + hint
    hint = (
        " Token exchange succeeded, so the most likely issue is env mismatch: "
        "SHOPIFY_STORE points to a different shop than the Dev Dashboard app/token, "
        "or the wrong client ID/secret pair is configured in Production."
        " Verify the app is installed on this exact store, the client ID and secret come "
        "from the same app, and redeploy after fixing env."
    )
    return base + hint


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
        err = _format_shopify_token_error(r)
        raise RuntimeError(
            f"Shopify client credentials failed ({r.status_code}): {err}"
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


def _format_shopify_token_error(r: requests.Response) -> str:
    """Short message; Shopify often returns HTML error pages instead of JSON."""
    text = (r.text or "").strip()
    if "app_not_installed" in text:
        return (
            "app_not_installed — the Dev Dashboard app is not installed on this shop "
            f"({_shop_from_oauth_url(r)}). "
            "Install the app on the store (Shopify Admin → Settings → Apps), "
            "or use SHOPIFY_TOKEN from a custom app and remove SHOPIFY_CLIENT_ID/SECRET."
        )
    if "application_cannot_be_found" in text.lower():
        m = _HTML_OAUTH_ERR.search(text)
        detail = f" ({m.group(1)}: {m.group(2).strip()})" if m else ""
        return (
            "application_cannot_be_found — per Shopify, client_credentials on "
            "/admin/oauth/access_token is for apps from the Dev Dashboard (Partners); "
            "merchant custom apps (Admin → Develop apps) must use token exchange or an "
            "Admin API access token. Fix: set SHOPIFY_TOKEN to the shpat_ token from "
            "Develop apps → API credentials and remove SHOPIFY_CLIENT_ID/SECRET; OR create "
            "an app in partners.shopify.com, install it on the store, and use that app's "
            "Client ID + Secret in GitHub. See Shopify docs: client-credentials-grant."
            + detail
        )
    m = _HTML_OAUTH_ERR.search(text)
    if m:
        return f"{m.group(1)}: {m.group(2).strip()}"
    try:
        body = r.json()
        if isinstance(body, dict):
            return str(body.get("error_description") or body.get("error") or json.dumps(body)[:400])
    except (ValueError, TypeError):
        pass
    return text[:600] + ("…" if len(text) > 600 else "")


def _shop_from_oauth_url(r: requests.Response) -> str:
    from urllib.parse import urlparse

    host = urlparse(r.url).netloc or "store"
    return host.split(".")[0] if "." in host else host
