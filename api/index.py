"""Vercel Python entrypoint: must be named index.py (or app.py, …) and export `app`."""

from __future__ import annotations

import logging
import os
import sys

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from . import ui
from .dashboard import (
    bookkeeping_table,
    dataframe_to_json_records,
    load_dashboard_bundle,
    marketing_campaign_table,
    missing_costs_table,
    recent_daily_table,
    recent_orders_table,
    run_status_rows,
    summary_cards,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecom_profit_engine.api")

app = FastAPI(title="Ecom Profit Engine")

_STATIC_APP_DIR = os.path.join(_ROOT, "static", "app")
_STATIC_APP_ASSETS = os.path.join(_STATIC_APP_DIR, "assets")
_STATIC_APP_NEXT = os.path.join(_STATIC_APP_DIR, "_next")
if os.path.isdir(_STATIC_APP_ASSETS):
    app.mount("/app/assets", StaticFiles(directory=_STATIC_APP_ASSETS), name="app_assets")
if os.path.isdir(_STATIC_APP_NEXT):
    app.mount("/app/_next", StaticFiles(directory=_STATIC_APP_NEXT), name="app_next_static")


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


def _require_app_session_api(request: Request) -> None:
    """401 JSON keď je CRON_SECRET ale chýba prihlásenie."""
    if not _cron_secret():
        return
    sess = getattr(request, "session", None)
    if sess is not None and sess.get("app_auth"):
        return
    raise HTTPException(status_code=401, detail="Unauthorized")


def _spa_export_html_path(request: Request) -> str:
    """Next `output: export` emits `index.html` plus `{route}.html` per top-level segment."""
    path = request.url.path.rstrip("/") or "/app"
    if path == "/app":
        return os.path.join(_STATIC_APP_DIR, "index.html")
    if path.startswith("/app/"):
        segment = path.removeprefix("/app/").split("/")[0]
        allowed = {"orders", "daily", "marketing", "accounting", "costs", "jobs"}
        if segment in allowed:
            candidate = os.path.join(_STATIC_APP_DIR, f"{segment}.html")
            if os.path.isfile(candidate):
                return candidate
    return os.path.join(_STATIC_APP_DIR, "index.html")


def _spa_index_response(request: Request) -> FileResponse:
    path = _spa_export_html_path(request)
    if not os.path.isfile(path):
        raise HTTPException(
            status_code=503,
            detail="Frontend nie je zbuildovaný. Spusti: cd dashboard-web && npm ci && npm run build",
        )
    return FileResponse(path)


def _legacy_app_html(request: Request) -> Response:
    """HTML fallback keď chýba React build."""
    bundle = load_dashboard_bundle()
    path = request.url.path.rstrip("/") or "/app"
    if path == "/app":
        return HTMLResponse(
            ui.page_dashboard(
                status=ui.ui_status(),
                cards=summary_cards(bundle),
                runs=run_status_rows(bundle),
                recent_orders=recent_orders_table(bundle),
                recent_daily=recent_daily_table(bundle),
            )
        )
    if path == "/app/orders":
        return HTMLResponse(ui.page_orders(recent_orders_table(bundle, limit=100)))
    if path == "/app/daily":
        return HTMLResponse(ui.page_daily(recent_daily_table(bundle, limit=100)))
    if path == "/app/marketing":
        rd = recent_daily_table(bundle, limit=100)
        meta_cols = [c for c in rd.columns if c in ("Date", "Ad_Spend", "Marketing_ROAS")]
        meta_table = rd[meta_cols] if not rd.empty and meta_cols else rd
        return HTMLResponse(ui.page_marketing(meta_table, marketing_campaign_table(bundle, limit=100)))
    if path == "/app/accounting":
        return HTMLResponse(ui.page_accounting(bookkeeping_table(bundle, limit=36)))
    if path == "/app/costs":
        return HTMLResponse(ui.page_costs(missing_costs_table(bundle, limit=100)))
    if path == "/app/jobs":
        return HTMLResponse(ui.page_jobs(run_status_rows(bundle)))
    raise HTTPException(status_code=404, detail="Not found")


def _app_shell_response(request: Request) -> Response:
    redir = _require_app_user(request)
    if redir:
        return redir
    if os.path.isfile(os.path.join(_STATIC_APP_DIR, "index.html")):
        return _spa_index_response(request)
    return _legacy_app_html(request)


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


def _masked_prefix(value: str, length: int = 10) -> str:
    s = value.strip()
    if not s:
        return ""
    return s[:length]


def _shopify_env_debug_payload() -> dict[str, object]:
    from config import load_settings

    settings = load_settings()
    mode = "SHOPIFY_TOKEN" if settings.shopify_token else "client_credentials"
    token = settings.shopify_token or ""
    client_id = settings.shopify_client_id or ""
    client_secret = settings.shopify_client_secret or ""
    return {
        "shopify_store": settings.shopify_store,
        "shopify_api_version": settings.shopify_api_version,
        "auth_mode": mode,
        "has_shopify_token": bool(token),
        "shopify_token_len": len(token),
        "shopify_token_prefix": _masked_prefix(token),
        "has_client_id": bool(client_id),
        "client_id_len": len(client_id),
        "client_id_prefix": _masked_prefix(client_id),
        "has_client_secret": bool(client_secret),
        "client_secret_len": len(client_secret),
        "client_secret_prefix": _masked_prefix(client_secret),
    }


# Vercel serverless request body limit is ~4.5 MB; stay under it.
_MAX_BILL_UPLOAD_BYTES = 4 * 1024 * 1024


def _supplier_import_auto_pipeline_enabled() -> bool:
    v = (os.environ.get("SUPPLIER_IMPORT_AUTO_PIPELINE") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _pipeline_after_import() -> dict[str, object]:
    """
    Po importe BillDetail spusti celý pipeline (predvolene zapnuté).
    Pri SUPPLIER_COSTS_FROM_CSV=1 nespúšťaj — pipeline by nečítal nové náklady zo Sheet.
    """
    out: dict[str, object] = {
        "pipeline_ran": False,
        "pipeline_ok": None,
        "pipeline_exit_code": None,
        "pipeline_error": None,
        "pipeline_skipped_reason": None,
    }
    if not _supplier_import_auto_pipeline_enabled():
        out["pipeline_skipped_reason"] = "auto_pipeline_disabled"
        return out
    from config import load_settings

    settings = load_settings()
    if not settings.supplier_costs_sheet_tab:
        out["pipeline_skipped_reason"] = "SUPPLIER_COSTS_FROM_CSV"
        return out
    try:
        from pipeline import main

        code = main()
        out["pipeline_ran"] = True
        out["pipeline_exit_code"] = code
        out["pipeline_ok"] = code == 0
        return out
    except Exception as exc:
        logger.exception("Pipeline after supplier import failed")
        out["pipeline_ran"] = True
        out["pipeline_ok"] = False
        out["pipeline_error"] = str(exc)
        return out


def _pipeline_after_import_html_lines(pipe: dict[str, object]) -> list[str]:
    lines: list[str] = []
    reason = pipe.get("pipeline_skipped_reason")
    if reason == "auto_pipeline_disabled":
        lines.append("")
        lines.append("Automatický report je vypnutý (SUPPLIER_IMPORT_AUTO_PIPELINE=0).")
    elif reason == "SUPPLIER_COSTS_FROM_CSV":
        lines.append("")
        lines.append(
            "Automatický report sa preskočil: máš SUPPLIER_COSTS_FROM_CSV=1 — pipeline číta CSV, nie záložku v Sheet. "
            "Vypni SUPPLIER_COSTS_FROM_CSV na Verceli, aby sa po nahratí použili nové náklady."
        )
    elif pipe.get("pipeline_ran"):
        lines.append("")
        lines.append("---")
        if pipe.get("pipeline_ok"):
            lines.append("Report (Shopify + Meta → všetky záložky) bol úspešne aktualizovaný.")
        elif pipe.get("pipeline_error"):
            lines.append(f"Report zlyhal (náklady sú už v Sheet): {pipe['pipeline_error']}")
        else:
            lines.append(
                f"Report: pipeline skončil s kódom {pipe.get('pipeline_exit_code')}. Skús spustiť znova z Domov."
            )
    return lines


async def _run_supplier_bill_import(file: UploadFile) -> dict[str, object]:
    """Parse BillDetail file and overwrite supplier costs worksheet. Raises HTTPException."""
    from config import DEFAULT_SUPPLIER_COSTS_SHEET_TAB, load_settings

    settings = load_settings()
    # Záložka sa vytvorí automaticky ak neexistuje; pri SUPPLIER_COSTS_FROM_CSV ide zápis do predvolenej záložky.
    tab = (settings.supplier_costs_sheet_tab or "").strip() or DEFAULT_SUPPLIER_COSTS_SHEET_TAB

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
        from bill_detail_import import (
            bill_detail_dataframe_to_supplier_costs,
            bill_detail_single_item_order_rows,
            read_bill_detail_sheet,
        )
        from sheets import replace_worksheet_simple

        raw = read_bill_detail_sheet(content, file.filename or "export.xls")
        df = bill_detail_dataframe_to_supplier_costs(raw)
        single_df = bill_detail_single_item_order_rows(raw)
        if df.empty:
            raise ValueError(
                "No product lines parsed. Check ProductInfo format (SKU,Title:qty(price))."
            )
        replace_worksheet_simple(settings, tab, df)
        if settings.supplier_bill_single_orders_tab and not single_df.empty:
            replace_worksheet_simple(
                settings, settings.supplier_bill_single_orders_tab, single_df
            )
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
        "single_item_order_rows": len(single_df.index),
    }


@app.get("/", response_model=None)
def root(request: Request) -> dict[str, str] | RedirectResponse:
    # Prehliadač na úvodnej URL dostane rozhranie namiesto holého JSON.
    accept = (request.headers.get("accept") or "").lower()
    if "text/html" in accept:
        return RedirectResponse(url="/app", status_code=302)
    return {
        "service": "ecom-profit-engine",
        "app": "/app",
        "pipeline": "/cron",
        "import_supplier_bill": "/import-bill-detail",
    }


@app.get("/api/app/dashboard", response_model=None)
def api_app_dashboard(request: Request) -> JSONResponse:
    _require_app_session_api(request)
    bundle = load_dashboard_bundle()
    return JSONResponse(
        {
            "status": ui.ui_status(),
            "cards": summary_cards(bundle),
            "runs": run_status_rows(bundle),
            "recent_orders": dataframe_to_json_records(recent_orders_table(bundle)),
            "recent_daily": dataframe_to_json_records(recent_daily_table(bundle)),
        }
    )


@app.get("/api/app/orders", response_model=None)
def api_app_orders(request: Request) -> JSONResponse:
    _require_app_session_api(request)
    bundle = load_dashboard_bundle()
    return JSONResponse({"rows": dataframe_to_json_records(recent_orders_table(bundle, limit=100))})


@app.get("/api/app/daily", response_model=None)
def api_app_daily(request: Request) -> JSONResponse:
    _require_app_session_api(request)
    bundle = load_dashboard_bundle()
    return JSONResponse({"rows": dataframe_to_json_records(recent_daily_table(bundle, limit=100))})


@app.get("/api/app/marketing", response_model=None)
def api_app_marketing(request: Request) -> JSONResponse:
    _require_app_session_api(request)
    bundle = load_dashboard_bundle()
    rd = recent_daily_table(bundle, limit=100)
    meta_cols = [c for c in rd.columns if c in ("Date", "Ad_Spend", "Marketing_ROAS")]
    meta_table = rd[meta_cols] if not rd.empty and meta_cols else rd
    return JSONResponse(
        {
            "meta_daily": dataframe_to_json_records(meta_table),
            "campaigns": dataframe_to_json_records(marketing_campaign_table(bundle, limit=100)),
        }
    )


@app.get("/api/app/accounting", response_model=None)
def api_app_accounting(request: Request) -> JSONResponse:
    _require_app_session_api(request)
    bundle = load_dashboard_bundle()
    return JSONResponse({"rows": dataframe_to_json_records(bookkeeping_table(bundle, limit=36))})


@app.get("/api/app/costs", response_model=None)
def api_app_costs(request: Request) -> JSONResponse:
    _require_app_session_api(request)
    bundle = load_dashboard_bundle()
    return JSONResponse({"rows": dataframe_to_json_records(missing_costs_table(bundle, limit=100))})


@app.get("/api/app/jobs", response_model=None)
def api_app_jobs(request: Request) -> JSONResponse:
    _require_app_session_api(request)
    bundle = load_dashboard_bundle()
    return JSONResponse({"runs": run_status_rows(bundle)})


@app.post("/api/app/run/{mode}", response_model=None)
def api_app_run(request: Request, mode: str) -> JSONResponse:
    _require_app_session_api(request)
    mode_norm = (mode or "").strip().lower()
    if mode_norm not in {"full", "core", "tracking", "reporting"}:
        raise HTTPException(status_code=404, detail="Unknown pipeline mode")
    try:
        from pipeline import main

        code = main(mode_norm)
        ok = code == 0
        msg = (
            f"Pipeline mód {mode_norm} sa úspešne aktualizoval."
            if ok
            else f"Pipeline mód {mode_norm} skončil s návratovým kódom {code}."
        )
        return JSONResponse(
            status_code=200 if ok else 500,
            content={"ok": ok, "exitCode": code, "mode": mode_norm, "message": msg},
        )
    except Exception as exc:
        logger.exception("App pipeline failed for mode=%s", mode_norm)
        return JSONResponse(
            status_code=500,
            content={"ok": False, "mode": mode_norm, "error": str(exc)},
        )


@app.get("/app", response_model=None)
@app.get("/app/", response_model=None)
@app.get("/app/orders", response_model=None)
@app.get("/app/daily", response_model=None)
@app.get("/app/marketing", response_model=None)
@app.get("/app/accounting", response_model=None)
@app.get("/app/costs", response_model=None)
@app.get("/app/jobs", response_model=None)
def app_shell(request: Request) -> Response:
    return _app_shell_response(request)


@app.get("/app/favicon.ico", response_model=None)
def app_favicon() -> FileResponse:
    path = os.path.join(_STATIC_APP_DIR, "favicon.ico")
    if os.path.isfile(path):
        return FileResponse(path)
    raise HTTPException(status_code=404, detail="Not found")


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
    return RedirectResponse(url="/app/costs", status_code=302)


def _run_pipeline_mode_html(mode: str, back_href: str = "/app/jobs") -> HTMLResponse:
    try:
        from pipeline import main

        code = main(mode)
        ok = code == 0
        msg = (
            f"Pipeline mód {mode} sa úspešne aktualizoval."
            if ok
            else f"Pipeline mód {mode} skončil s návratovým kódom {code}."
        )
        body = ui.page_message(ok=ok, title=f"Run {mode}", message=msg, back_href=back_href)
        return HTMLResponse(body, status_code=200 if ok else 500)
    except Exception as exc:
        logger.exception("App pipeline failed for mode=%s", mode)
        body = ui.page_message(ok=False, title=f"Chyba pipeline ({mode})", message=str(exc), back_href=back_href)
        return HTMLResponse(body, status_code=500)


@app.post("/app/report", response_class=HTMLResponse, response_model=None)
def app_report(request: Request) -> Response:
    redir = _require_app_user(request)
    if redir:
        return redir
    return _run_pipeline_mode_html("full", back_href="/app")


@app.post("/app/run/{mode}", response_class=HTMLResponse, response_model=None)
def app_run_mode(request: Request, mode: str) -> Response:
    redir = _require_app_user(request)
    if redir:
        return redir
    mode_norm = (mode or "").strip().lower()
    if mode_norm not in {"full", "core", "tracking", "reporting"}:
        raise HTTPException(status_code=404, detail="Unknown pipeline mode")
    return _run_pipeline_mode_html(mode_norm)


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


@app.api_route("/cron/core", methods=["GET", "POST"])
def run_pipeline_core(request: Request) -> JSONResponse:
    _check_auth(request)
    try:
        from pipeline import main

        code = main("core")
        return JSONResponse(status_code=200 if code == 0 else 500, content={"ok": code == 0, "exitCode": code, "mode": "core"})
    except Exception as exc:
        logger.exception("Pipeline core failed")
        logger.error("pipeline_error=%s", str(exc).replace("\n", " ")[:2000])
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc), "mode": "core"})


@app.api_route("/cron/tracking", methods=["GET", "POST"])
def run_pipeline_tracking(request: Request) -> JSONResponse:
    _check_auth(request)
    try:
        from pipeline import main

        code = main("tracking")
        return JSONResponse(status_code=200 if code == 0 else 500, content={"ok": code == 0, "exitCode": code, "mode": "tracking"})
    except Exception as exc:
        logger.exception("Pipeline tracking failed")
        logger.error("pipeline_error=%s", str(exc).replace("\n", " ")[:2000])
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc), "mode": "tracking"})


@app.api_route("/cron/reporting", methods=["GET", "POST"])
def run_pipeline_reporting(request: Request) -> JSONResponse:
    _check_auth(request)
    try:
        from pipeline import main

        code = main("reporting")
        return JSONResponse(status_code=200 if code == 0 else 500, content={"ok": code == 0, "exitCode": code, "mode": "reporting"})
    except Exception as exc:
        logger.exception("Pipeline reporting failed")
        logger.error("pipeline_error=%s", str(exc).replace("\n", " ")[:2000])
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc), "mode": "reporting"})


@app.get("/debug/shopify-env")
def debug_shopify_env(request: Request) -> JSONResponse:
    """Sanitized runtime view of Shopify auth env. Protected by the same auth as /cron."""
    _check_auth(request)
    try:
        return JSONResponse(status_code=200, content={"ok": True, **_shopify_env_debug_payload()})
    except Exception as exc:
        logger.exception("Shopify env debug failed")
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})


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
    pipe = _pipeline_after_import()
    lines = [
        f"Hotovo. Zapísaných produktov: {result['rows']}",
        f"Záložka: {result['tab']}",
        f"Tabuľka: {result['spreadsheet']}",
    ]
    lines.extend(_pipeline_after_import_html_lines(pipe))
    pipeline_failed = bool(
        pipe.get("pipeline_ran") and pipe.get("pipeline_ok") is False
    )
    body = ui.page_message(
        ok=not pipeline_failed,
        title="Import a report",
        message="\n".join(lines).strip(),
        back_href="/app",
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
    pipe = _pipeline_after_import()
    payload = {**result, **pipe}
    pipeline_failed = bool(
        pipe.get("pipeline_ran") and pipe.get("pipeline_ok") is False
    )
    return JSONResponse(
        status_code=500 if pipeline_failed else 200,
        content=payload,
    )
