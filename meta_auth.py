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

    If META_APP_ID and META_APP_SECRET are set, exchanges META_TOKEN for a fresh
    long-lived user token (~60 days per Meta). Use the same user token you get
    from Graph API Explorer (short or long) as META_TOKEN.

    If app id/secret are omitted, META_TOKEN is used as-is (you must rotate it manually).
    """
    if not settings.meta_app_id or not settings.meta_app_secret:
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
        raise RuntimeError(
            f"Meta token exchange failed ({r.status_code}){hint}: {msg}. "
            "Check META_TOKEN (user token with ads_read), META_APP_ID, META_APP_SECRET."
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
