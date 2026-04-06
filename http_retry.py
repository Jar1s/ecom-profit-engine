"""HTTP GET with retries and exponential backoff for transient failures."""

from __future__ import annotations

import random
import time
from typing import Any

import requests

from config import Settings


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
                            raise RuntimeError(f"HTTP {response.status_code}{hint}: {msg}") from None
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
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            _sleep_backoff(settings, attempt)
            attempt += 1
    assert last_exc is not None
    raise last_exc


def _sleep_backoff(settings: Settings, attempt: int) -> None:
    base = settings.http_backoff_base_seconds * (2**attempt)
    jitter = random.uniform(0, 0.25 * base)
    time.sleep(base + jitter)
