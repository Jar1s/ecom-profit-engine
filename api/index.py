"""Vercel Python entrypoint: must be named index.py (or app.py, …) and export `app`."""

from __future__ import annotations

import logging
import os
import sys

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.middleware.sessions import SessionMiddleware

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from . import ui

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecom_profit_engine.api")

app = FastAPI(title="Ecom Profit Engine")


def _session_signing_secret() -> str:
    """Podpis cookies relácie; nastav APP_SESSION_SECRET alebo použi CRON_SECRET."""
    s = (os.environ.get("APP_SESSION_SECRET") or os.environ.get("CRON_SECRET") or "").strip()
    if s:
        return s
    return "ecom-profit-dev-session-insecure"


app.add_middleware(
    SessionMiddleware,
    secret_key=_session_signing_secret(),
    max_age=60 * 60 * 24 * 14,
    same_site="lax",
)


def _cron_secret() -> str:
    return (os.environ.get("CRON_SECRET") or "").strip()


def _require_app_user(request: Request) -> RedirectResponse | None:
    """
    Ak je nastavené CRON_SECRET, vyžaduj prihlásenie cez /app/login (cookie relácia).
    Ak nie je nastavené, rozhranie je otvorené (vhodné len na lokálny test).
    """
    if not _cron_secret():
        return None
    sess = getattr(request, "session", None)
    if sess is not None and sess.get("app_auth"):
        return None
    return RedirectResponse(url="/app/login", status_code=302)


def _check_auth_import(request: Request, form_token: str | None = None) -> None:
    """
    Bearer, formulárové ``token``, relácia po prihlásení na ``/app/login``, alebo Cron hlavička.
    """
    secret = (os.environ.get("CRON_SECRET") or "").strip()
    if not secret:
        return
    sess = getattr(request, "session", None)
    if sess is not None and sess.get("app_auth"):
        return
    if form_token is not None and form_token.strip() == secret:
        return
    auth = (request.headers.get("authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        if auth[7:].strip() == secret:
            return
    if request.headers.get("x-vercel-cron") == "1":
        return
    raise HTTPException(status_code=401, detail="Unauthorized")


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


# Vercel serverless request body limit is ~4.5 MB; stay under it.
_MAX_BILL_UPLOAD_BYTES = 4 * 1024 * 1024


async def _run_supplier_bill_import(file: UploadFile) -> dict[str, object]:
    """Parse BillDetail file and overwrite supplier costs worksheet. Raises HTTPException."""
    from config import load_settings

    settings = load_settings()
    tab = (settings.supplier_costs_sheet_tab or "").strip()
    if not tab:
        raise HTTPException(
            status_code=400,
            detail=(
                "Nastav SUPPLIER_COSTS_SHEET_TAB (názov záložky v Google Sheet, napr. SUPPLIER_COSTS)."
            ),
        )

    name = (file.filename or "").strip().lower()
    if not name.endswith((".xls", ".xlsx")):
        raise HTTPException(
            status_code=400,
            detail="Vyber súbor .xls alebo .xlsx (BillDetail export).",
        )

    content = await file.read()
    if len(content) > _MAX_BILL_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail="Súbor je príliš veľký (max. cca 4 MB na Verceli).",
        )

    try:
        from bill_detail_import import bill_detail_bytes_to_supplier_costs_df
        from sheets import replace_worksheet_simple

        df = bill_detail_bytes_to_supplier_costs_df(content, file.filename or "export.xls")
        replace_worksheet_simple(settings, tab, df)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("supplier bill import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "ok": True,
        "rows": len(df.index),
        "tab": tab,
        "spreadsheet": settings.google_sheet_id or settings.google_sheet_name,
    }


@app.get("/")
def health() -> dict[str, str]:
    # On Vercel, routes are at domain root (this file is api/index.py but "/" is the app root).
    return {
        "service": "ecom-profit-engine",
        "app": "/app",
        "pipeline": "/cron",
        "import_supplier_bill": "/import-bill-detail",
    }


@app.get("/app", response_class=HTMLResponse, response_model=None)
def app_home(request: Request) -> Response:
    redir = _require_app_user(request)
    if redir:
        return redir
    return HTMLResponse(ui.page_dashboard(ui.ui_status()))


@app.get("/app/login", response_class=HTMLResponse, response_model=None)
def app_login_get(request: Request) -> Response:
    if not _cron_secret():
        return RedirectResponse(url="/app", status_code=302)
    sess = getattr(request, "session", None)
    if sess is not None and sess.get("app_auth"):
        return RedirectResponse(url="/app", status_code=302)
    return HTMLResponse(ui.page_login())


@app.post("/app/login", response_model=None)
def app_login_post(request: Request, password: str = Form(...)) -> Response:
    if not _cron_secret():
        return RedirectResponse(url="/app", status_code=302)
    if password.strip() != _cron_secret():
        return HTMLResponse(ui.page_login(error="Nesprávne heslo."), status_code=401)
    request.session["app_auth"] = True
    return RedirectResponse(url="/app", status_code=302)


@app.get("/app/logout")
def app_logout(request: Request) -> RedirectResponse:
    request.session.clear()
    if _cron_secret():
        return RedirectResponse(url="/app/login", status_code=302)
    return RedirectResponse(url="/app", status_code=302)


@app.get("/app/naklady", response_class=HTMLResponse, response_model=None)
def app_naklady(request: Request) -> Response:
    redir = _require_app_user(request)
    if redir:
        return redir
    return HTMLResponse(ui.page_naklady())


@app.post("/app/report", response_class=HTMLResponse, response_model=None)
def app_report(request: Request) -> Response:
    redir = _require_app_user(request)
    if redir:
        return redir
    try:
        from pipeline import main

        code = main()
        ok = code == 0
        msg = (
            "Report sa úspešne aktualizoval."
            if ok
            else f"Pipeline skončil s návratovým kódom {code}."
        )
        body = ui.page_message(
            ok=ok,
            title="Report",
            message=msg,
            back_href="/app",
        )
        return HTMLResponse(body, status_code=200 if ok else 500)
    except Exception as exc:
        logger.exception("App pipeline failed")
        body = ui.page_message(
            ok=False,
            title="Chyba pipeline",
            message=str(exc),
            back_href="/app",
        )
        return HTMLResponse(body, status_code=500)


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


@app.get("/supplier-import")
def supplier_import_legacy_redirect() -> RedirectResponse:
    """Starý odkaz → nové rozhranie."""
    return RedirectResponse(url="/app/naklady", status_code=302)


@app.post("/supplier-import")
async def supplier_import_submit(
    request: Request,
    file: UploadFile = File(...),
    token: str | None = Form(None),
) -> HTMLResponse:
    """Import z prehliadača (relácia po /app/login) alebo s poleom ``token`` / Bearer."""
    _check_auth_import(request, token)
    try:
        result = await _run_supplier_bill_import(file)
    except HTTPException as exc:
        detail = exc.detail
        msg = detail if isinstance(detail, str) else str(detail)
        body = ui.page_message(
            ok=False,
            title="Chyba",
            message=msg,
            back_href="/app/naklady",
        )
        return HTMLResponse(body, status_code=exc.status_code)
    lines = [
        f"Hotovo. Zapísaných produktov: {result['rows']}",
        f"Záložka: {result['tab']}",
        f"Tabuľka: {result['spreadsheet']}",
    ]
    body = ui.page_message(
        ok=True,
        title="Náklady doplnené",
        message="\n".join(lines),
        back_href="/app/naklady",
    )
    return HTMLResponse(body, status_code=200)


@app.post("/import-bill-detail")
async def import_bill_detail(
    request: Request,
    file: UploadFile = File(..., description="BillDetail .xls or .xlsx export"),
    token: str | None = Form(None),
) -> JSONResponse:
    """
    Upload supplier BillDetail export; parse and overwrite ``SUPPLIER_COSTS_SHEET_TAB``.
    Auth: Bearer ``CRON_SECRET``, optional form field ``token`` (same secret), or Vercel Cron header.
    Webové rozhranie: ``GET /app`` → Náklady dodávateľa.
    """
    _check_auth_import(request, token)
    try:
        result = await _run_supplier_bill_import(file)
    except HTTPException:
        raise
    return JSONResponse(status_code=200, content=result)
