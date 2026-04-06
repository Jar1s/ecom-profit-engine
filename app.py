"""Vercel / FastAPI entry: GET/POST /api/cron runs the pipeline (replaces api/cron.py handler)."""

from __future__ import annotations

import logging
import os
import sys

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

_ROOT = os.path.dirname(os.path.abspath(__file__))
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
    secret = os.environ.get("CRON_SECRET", "").strip()
    if not secret:
        return
    if (authorization or "").strip() != f"Bearer {secret}":
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/")
def health() -> dict[str, str]:
    return {"service": "ecom-profit-engine", "cron": "/api/cron"}


@app.api_route("/api/cron", methods=["GET", "POST"])
def run_pipeline(authorization: str | None = Header(default=None)) -> JSONResponse:
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
