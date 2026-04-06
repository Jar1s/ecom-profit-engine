"""Vercel Serverless entry: scheduled cron hits GET /api/cron."""

from __future__ import annotations

import json
import logging
import os
import sys
from http.server import BaseHTTPRequestHandler

# Project root (parent of /api)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _authorized(headers: dict[str, str]) -> bool:
    """Vercel Cron sends Authorization: Bearer <CRON_SECRET> when CRON_SECRET is set."""
    secret = os.environ.get("CRON_SECRET", "").strip()
    if not secret:
        return True
    auth = headers.get("Authorization") or headers.get("authorization") or ""
    return auth == f"Bearer {secret}"


class handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 — Vercel expects this name
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def _handle(self) -> None:
        hdrs = {k: v for k, v in self.headers.items()}
        if not _authorized(hdrs):
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "error": "Unauthorized"}).encode("utf-8"))
            return

        try:
            from main import main

            code = main()
            body = json.dumps({"ok": code == 0, "exitCode": code}).encode("utf-8")
            self.send_response(200 if code == 0 else 500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        except Exception as exc:
            logging.exception("Pipeline failed")
            err = json.dumps({"ok": False, "error": str(exc)}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(err)

    def log_message(self, format: str, *args: object) -> None:
        logging.getLogger("vercel.cron").info("%s - %s", self.address_string(), format % args)
