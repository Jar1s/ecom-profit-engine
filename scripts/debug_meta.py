#!/usr/bin/env python3
"""Test Meta token: optional fb_exchange, then GET /me on Graph API.

  cd ecom-profit-engine && python scripts/debug_meta.py

Does not print the full token.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import requests

from config import load_settings
from meta_auth import get_meta_access_token


def main() -> int:
    settings = load_settings()
    ex = (
        settings.meta_app_id
        and settings.meta_app_secret
        and settings.meta_fb_exchange
    )
    print(f"META_FB_EXCHANGE={settings.meta_fb_exchange} will_exchange={bool(ex)}")
    try:
        token = get_meta_access_token(settings)
    except RuntimeError as e:
        print(f"token_step: FAIL {e}")
        return 1
    print(f"token_len={len(token)} ok_after_exchange_or_direct=1")
    url = f"https://graph.facebook.com/{settings.meta_api_version}/me"
    r = requests.get(url, params={"access_token": token}, timeout=60)
    print(f"GET /me HTTP {r.status_code}")
    if not r.ok:
        print((r.text or "")[:400])
        return 1
    print("ok:", r.json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
