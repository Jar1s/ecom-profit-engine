"""Vercel Python entrypoint: must be named index.py (or app.py, …) and export `app`."""

from __future__ import annotations

import logging
import os
import sys

from fastapi import FastAPI, Header, HTTPException
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


def _check_auth(authorization: str | None) -> None:
    """Compare bearer token to CRON_SECRET; tolerate extra spaces after 'Bearer'."""
    secret = (os.environ.get("CRON_SECRET") or "").strip()
    if not secret:
        return
    raw = (authorization or "").strip()
    if not raw.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    token = raw[7:].strip()
    if token != secret:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/")
def health() -> dict[str, str]:
    # On Vercel, routes are at domain root (this file is api/index.py but "/" is the app root).
    return {"service": "ecom-profit-engine", "pipeline": "/cron"}


@app.api_route("/cron", methods=["GET", "POST"])
def run_pipeline(authorization: str | None = Header(default=None)) -> JSONResponse:
    """On Vercel, call GET/POST https://<deployment>/cron (with CRON_SECRET if set)."""
    _check_auth(authorization)
    try:
        from pipeline import main

        code = main()
        body: dict = {"ok": code == 0, "exitCode": code}
        return JSONResponse(status_code=200 if code == 0 else 500, content=body)
    except Exception as exc:
        logger.exception("Pipeline failed")
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(exc)},
        )
