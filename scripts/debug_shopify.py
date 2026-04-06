#!/usr/bin/env python3
"""One-shot Shopify check: loads the same env as pipeline, GET orders.json?limit=1.

Run from ecom-profit-engine/ with .env or exported vars (same as Vercel):

  cd ecom-profit-engine && python scripts/debug_shopify.py

Does not print secrets; prints HTTP status and a short error body on failure.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import requests

from config import load_settings
from shopify_auth import get_shopify_access_token


def main() -> int:
    settings = load_settings()
    token = get_shopify_access_token(settings)
    url = (
        f"https://{settings.shopify_store}/admin/api/"
        f"{settings.shopify_api_version}/orders.json?limit=1"
    )
    mode = "SHOPIFY_TOKEN" if settings.shopify_token else "client_credentials"
    print(f"store={settings.shopify_store!r} api={settings.shopify_api_version} auth={mode}")
    print(f"token_len={len(token)} starts_with_shpat={token.startswith('shpat_')}")
    r = requests.get(
        url,
        headers={"X-Shopify-Access-Token": token},
        params={"status": "any", "limit": 1},
        timeout=60,
    )
    print(f"HTTP {r.status_code}")
    if r.ok:
        data = r.json()
        n = len(data.get("orders") or [])
        print(f"ok: orders in response={n}")
        return 0
    text = (r.text or "")[:500]
    print(f"body: {text}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
