"""Optional Meta long-lived token refresh via App ID + App Secret (fb_exchange_token)."""

from __future__ import annotations

import logging
from typing import Any

import requests

from config import Settings

logger = logging.getLogger(__name__)


def get_meta_access_token(settings: Settings) -> str:
    """
    Return access token for Graph API.

    If META_APP_ID, META_APP_SECRET, and META_FB_EXCHANGE (default true) are set,
    exchanges META_TOKEN via fb_exchange_token for a long-lived user token (~60d).

    If app id/secret are omitted, or META_FB_EXCHANGE is 0/false, META_TOKEN is
    used as-is (use a long-lived token from Business Manager / Graph Explorer).

    Exchange requires META_TOKEN to still be valid; if it expired (error 190),
    generate a new user token with ads_read and update META_TOKEN.
    """
    use_exchange = (
        bool(settings.meta_app_id)
        and bool(settings.meta_app_secret)
        and settings.meta_fb_exchange
    )
    if not use_exchange:
        return settings.meta_token
    return _exchange_token(settings)


def _exchange_token(settings: Settings) -> str:
    url = f"https://graph.facebook.com/{settings.meta_api_version}/oauth/access_token"
    params: dict[str, str] = {
        "grant_type": "fb_exchange_token",
        "client_id": settings.meta_app_id,
        "client_secret": settings.meta_app_secret,
        "fb_exchange_token": settings.meta_token,
    }
    r = requests.get(url, params=params, timeout=60)
    if not r.ok:
        code: int | None = None
        try:
            body = r.json()
            err = body.get("error", {})
            if isinstance(err, dict):
                msg = err.get("message", r.text)
                code = err.get("code")
                hint = f" (code {code})" if code is not None else ""
            else:
                msg = str(err)
                hint = ""
        except ValueError:
            msg = r.text
            hint = ""
        extra = ""
        if code == 190 or (isinstance(msg, str) and "expired" in msg.lower()):
            extra = (
                " META_TOKEN session expired — open Graph API Explorer, generate a new user "
                "token with ads_read, set META_TOKEN on Vercel, redeploy. "
                "Or set META_FB_EXCHANGE=0 and put a non-expired long-lived token in META_TOKEN."
            )
        raise RuntimeError(
            f"Meta token exchange failed ({r.status_code}){hint}: {msg}.{extra}"
        ) from None

    data: dict[str, Any] = r.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("Meta token exchange response missing access_token")
    expires_in = data.get("expires_in")
    if expires_in is not None:
        logger.info("Meta: exchanged token (expires_in=%s s)", expires_in)
    else:
        logger.info("Meta: exchanged token (no expires_in in response)")
    return str(token)
