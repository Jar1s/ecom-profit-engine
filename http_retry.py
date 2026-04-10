"""HTTP GET with retries and exponential backoff for transient failures."""

from __future__ import annotations

import random
import time
from typing import Any

import requests

from config import Settings

_SHOPIFY_403_HINT = (
    " In Shopify Admin → Settings → Apps → your app: add Admin API scope "
    "**read_orders** (and save), then **reinstall** the app on the store so the "
    "new token includes that scope."
)

_META_OAUTH_HINT = (
    " Renew META_TOKEN in Vercel (or local .env): short-lived Graph tokens expire. "
    "Use a long-lived user token with ads_read, or re-run token exchange with a fresh short-lived token. "
    "fb_exchange_token cannot refresh an already-expired session."
)


def _http_error_detail(response: requests.Response) -> str:
    """Short message from JSON body; Shopify often returns {\"errors\": \"...\"}."""
    text = (response.text or "").strip()
    if not text:
        return ""
    try:
        data = response.json()
    except ValueError:
        return text[:1200]
    if not isinstance(data, dict):
        return text[:1200]
    err = data.get("errors")
    if err is not None:
        if isinstance(err, str):
            return err
        if isinstance(err, dict):
            parts = [f"{k}: {v}" for k, v in err.items()]
            return "; ".join(parts)[:1200]
        if isinstance(err, list):
            return str(err)[:1200]
    err = data.get("error")
    if isinstance(err, str):
        return err
    if isinstance(err, dict):
        msg = err.get("message")
        if msg:
            return str(msg)
    return text[:1200]


def get_json(
    url: str,
    *,
    settings: Settings,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    """GET JSON; retry on 429, 5xx, and connection errors."""
    session = requests.Session()
    attempt = 0
    last_exc: Exception | None = None
    while attempt < settings.http_max_retries:
        try:
            response = session.get(url, params=params, headers=headers, timeout=120)
            if response.status_code == 429 or response.status_code >= 500:
                _sleep_backoff(settings, attempt)
                attempt += 1
                continue
            if response.status_code >= 400:
                try:
                    err_body = response.json()
                    if isinstance(err_body, dict) and "error" in err_body:
                        err = err_body["error"]
                        if isinstance(err, dict):
                            msg = err.get("message", str(err))
                            code = err.get("code")
                            hint = f" (code {code})" if code is not None else ""
                            tail = ""
                            if code in (190, 102):
                                tail = _META_OAUTH_HINT
                            raise RuntimeError(
                                f"HTTP {response.status_code}{hint}: {msg}{tail}"
                            ) from None
                except ValueError:
                    pass
                response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            _sleep_backoff(settings, attempt)
            attempt += 1
    assert last_exc is not None
    raise last_exc


def get_response(
    url: str,
    *,
    settings: Settings,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> requests.Response:
    """GET full response (for Link headers). Retries same as get_json."""
    session = requests.Session()
    attempt = 0
    last_exc: Exception | None = None
    while attempt < settings.http_max_retries:
        try:
            response = session.get(url, params=params, headers=headers, timeout=120)
            if response.status_code == 429 or response.status_code >= 500:
                _sleep_backoff(settings, attempt)
                attempt += 1
                continue
            if response.status_code >= 400:
                detail = _http_error_detail(response)
                msg = f"HTTP {response.status_code}"
                if detail:
                    msg = f"{msg}: {detail}"
                else:
                    msg = f"{msg} for {response.url}"
                if response.status_code == 403 and "myshopify.com" in (response.url or ""):
                    msg = msg + _SHOPIFY_403_HINT
                raise RuntimeError(msg) from None
            return response
        except requests.RequestException as exc:
            last_exc = exc
            _sleep_backoff(settings, attempt)
            attempt += 1
    assert last_exc is not None
    raise last_exc


def post_json_with_retry(
    settings: Settings,
    url: str,
    *,
    headers: dict[str, str],
    json_body: Any,
) -> Any:
    """
    POST JSON to any URL; retries on 429 / 5xx like :func:`get_response`.
    ``json_body`` may be a dict or list (e.g. 17TRACK uses a JSON array body).
    """
    session = requests.Session()
    attempt = 0
    last_exc: Exception | None = None
    while attempt < settings.http_max_retries:
        try:
            response = session.post(url, json=json_body, headers=headers, timeout=120)
            if response.status_code == 429 or response.status_code >= 500:
                _sleep_backoff(settings, attempt)
                attempt += 1
                continue
            if response.status_code >= 400:
                detail = _http_error_detail(response)
                msg = f"HTTP {response.status_code}"
                if detail:
                    msg = f"{msg}: {detail}"
                else:
                    msg = f"{msg} for {response.url}"
                if response.status_code == 403 and "myshopify.com" in (response.url or ""):
                    msg = msg + _SHOPIFY_403_HINT
                raise RuntimeError(msg) from None
            return response.json()
        except requests.RequestException as exc:
            last_exc = exc
            _sleep_backoff(settings, attempt)
            attempt += 1
    assert last_exc is not None
    raise last_exc


def post_graphql_admin(
    settings: Settings,
    *,
    query: str,
    variables: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    POST Shopify Admin GraphQL (`/admin/api/{version}/graphql.json`).
    Returns parsed JSON (HTTP 200); GraphQL-level errors are in the ``errors`` key.
    Retries on 429 / 5xx like :func:`get_response`.
    """
    shop = settings.shopify_store.strip()
    if shop.startswith("https://"):
        shop = shop.replace("https://", "").split("/")[0]
    url = f"https://{shop}/admin/api/{settings.shopify_api_version}/graphql.json"
    merged_headers = {"Content-Type": "application/json"}
    if headers:
        merged_headers.update(headers)
    body: dict[str, Any] = {"query": query}
    if variables is not None:
        body["variables"] = variables
    result = post_json_with_retry(settings, url, headers=merged_headers, json_body=body)
    return result if isinstance(result, dict) else {}


def _sleep_backoff(settings: Settings, attempt: int) -> None:
    base = settings.http_backoff_base_seconds * (2**attempt)
    jitter = random.uniform(0, 0.25 * base)
    time.sleep(base + jitter)
