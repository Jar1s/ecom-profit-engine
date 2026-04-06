"""Vercel Python entrypoint: must be named index.py (or app.py, …) and export `app`."""

from __future__ import annotations

import logging
import os
import sys

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecom_profit_engine.api")

app = FastAPI(title="Ecom Profit Engine")


def _check_auth(request: Request) -> None:
    """
    If CRON_SECRET is set: allow Bearer token, or Vercel Cron (x-vercel-cron: 1).

    Scheduled crons sometimes hit 401 when only Bearer is checked — the platform
    may not forward Authorization the same way as manual curl. The cron header is
    set by Vercel for scheduled invocations (external callers can spoof it; use
    obscure URL or remove CRON_SECRET for stricter Bearer-only checks).
    """
    secret = (os.environ.get("CRON_SECRET") or "").strip()
    if not secret:
        return
    auth = (request.headers.get("authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        if token == secret:
            return
    if request.headers.get("x-vercel-cron") == "1":
        return
    raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/")
def health() -> dict[str, str]:
    # On Vercel, routes are at domain root (this file is api/index.py but "/" is the app root).
    return {"service": "ecom-profit-engine", "pipeline": "/cron"}


@app.api_route("/cron", methods=["GET", "POST"])
def run_pipeline(request: Request) -> JSONResponse:
    """On Vercel, call GET/POST https://<deployment>/cron (with CRON_SECRET if set)."""
    _check_auth(request)
    try:
        from pipeline import main

        code = main()
        body: dict = {"ok": code == 0, "exitCode": code}
        return JSONResponse(status_code=200 if code == 0 else 500, content=body)
    except Exception as exc:
        logger.exception("Pipeline failed")
        # One-line summary for Vercel log search (full traceback is above)
        logger.error("pipeline_error=%s", str(exc).replace("\n", " ")[:2000])
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(exc)},
        )
