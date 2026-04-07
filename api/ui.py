"""HTML šablóny pre webové rozhranie (/app)."""

from __future__ import annotations

import html
import os
from typing import Any


def _sheet_url() -> str | None:
    sid = os.getenv("GOOGLE_SHEET_ID", "").strip()
    if not sid:
        return None
    return f"https://docs.google.com/spreadsheets/d/{sid}/edit"


def ui_status() -> dict[str, Any]:
    """Údaje na dashboard (bez načítania celého Settings)."""
    tab = os.getenv("SUPPLIER_COSTS_SHEET_TAB", "").strip()
    name = os.getenv("GOOGLE_SHEET_NAME", "").strip()
    return {
        "sheet_url": _sheet_url(),
        "supplier_tab": tab or None,
        "sheet_title_hint": name or None,
        "cron_secret_set": bool(os.getenv("CRON_SECRET", "").strip()),
    }


def _shell(
    *,
    title: str,
    body: str,
    active: str | None = None,
    show_nav: bool = True,
) -> str:
    nav_items = [
        ("Domov", "/app", "home"),
        ("Náklady dodávateľa", "/app/naklady", "naklady"),
    ]
    nav_html = ""
    if show_nav:
        parts = []
        for label, href, key in nav_items:
            cls = ' class="active"' if active == key else ""
            parts.append(f'<a href="{html.escape(href)}"{cls}>{html.escape(label)}</a>')
        nav_html = f'<nav class="nav">{" · ".join(parts)} · <a href="/app/logout">Odhlásiť</a></nav>'

    return f"""<!DOCTYPE html>
<html lang="sk">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <meta name="robots" content="noindex,nofollow"/>
  <title>{html.escape(title)} — Ecom Profit</title>
  <style>
    :root {{
      --bg: #0c0e12;
      --card: #151922;
      --text: #e8eaed;
      --muted: #9aa0a6;
      --accent: #3b82f6;
      --accent-hi: #60a5fa;
      --border: #2d3340;
      --ok: #34d399;
      --err: #f87171;
      font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; min-height: 100vh; background: var(--bg); color: var(--text);
      line-height: 1.5;
    }}
    .wrap {{ max-width: 42rem; margin: 0 auto; padding: 1.25rem 1rem 3rem; }}
    .brand {{ font-size: 1.1rem; font-weight: 700; letter-spacing: -0.02em; margin-bottom: 0.5rem; }}
    .brand span {{ color: var(--accent-hi); }}
    .nav {{
      font-size: 0.9rem; color: var(--muted); margin-bottom: 1.75rem; padding-bottom: 1rem; border-bottom: 1px solid var(--border);
    }}
    .nav a {{ color: var(--muted); text-decoration: none; }}
    .nav a:hover {{ color: var(--text); }}
    .nav a.active {{ color: var(--accent-hi); font-weight: 600; }}
    h1 {{ font-size: 1.35rem; font-weight: 650; margin: 0 0 0.75rem; }}
    p.lead {{ color: var(--muted); font-size: 0.95rem; margin: 0 0 1.5rem; }}
    .cards {{ display: grid; gap: 0.75rem; }}
    .card {{
      background: var(--card); border: 1px solid var(--border); border-radius: 12px;
      padding: 1rem 1.1rem;
    }}
    .card h2 {{ font-size: 0.95rem; margin: 0 0 0.35rem; font-weight: 600; }}
    .card p {{ margin: 0; font-size: 0.88rem; color: var(--muted); }}
    .card a.btn, button.btn {{
      display: inline-block; margin-top: 0.75rem; padding: 0.55rem 1rem; border-radius: 999px;
      background: var(--accent); color: #fff; text-decoration: none; font-weight: 600; font-size: 0.9rem;
      border: none; cursor: pointer;
    }}
    .card a.btn:hover, button.btn:hover {{ filter: brightness(1.08); }}
    .card a.btn.secondary {{ background: transparent; border: 1px solid var(--border); color: var(--text); }}
    label {{ display: block; margin-top: 0.9rem; font-size: 0.8rem; color: var(--muted); }}
    input[type="password"], input[type="file"] {{
      width: 100%; margin-top: 0.25rem; padding: 0.6rem 0.75rem; border-radius: 8px;
      border: 1px solid var(--border); background: #0f1218; color: var(--text);
    }}
    input[type="file"] {{ font-size: 0.85rem; }}
    .hint {{ font-size: 0.78rem; color: var(--muted); margin-top: 1rem; }}
    .status-line {{ font-size: 0.85rem; color: var(--muted); margin-top: 0.5rem; }}
    .status-line code {{ background: #0f1218; padding: 0.1rem 0.35rem; border-radius: 4px; }}
    pre.msg {{ white-space: pre-wrap; background: #0f1218; padding: 1rem; border-radius: 8px; font-size: 0.88rem; border: 1px solid var(--border); }}
    pre.msg.ok {{ border-color: #065f46; color: var(--ok); }}
    pre.msg.err {{ border-color: #7f1d1d; color: var(--err); }}
    .back {{ margin-top: 1.25rem; }}
    .back a {{ color: var(--accent-hi); }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="brand">Ecom <span>Profit</span></div>
    {nav_html}
    {body}
  </div>
</body>
</html>"""


def page_login(*, error: str | None = None) -> str:
    err = ""
    if error:
        err = f'<p class="lead" style="color:var(--err)">{html.escape(error)}</p>'
    body = f"""
    <h1>Prihlásenie</h1>
    <p class="lead">Zadaj rovnaké heslo ako máš v aplikácii nastavené ako <code>CRON_SECRET</code> (jednorazovo pri nasadení na Vercel — sem sa už nemusíš hrabať).</p>
    {err}
    <form method="post" action="/app/login">
      <label for="password">Heslo</label>
      <input type="password" id="password" name="password" autocomplete="current-password" required autofocus/>
      <button type="submit" class="btn" style="margin-top:1rem;width:100%">Prihlásiť sa</button>
    </form>
    <p class="hint">Ak <code>CRON_SECRET</code> nemáš nastavené, prihlásenie sa preskočí a apka je otvorená (vhodné len na test).</p>
    """
    return _shell(title="Prihlásenie", body=body, show_nav=False)


def page_dashboard(st: dict[str, Any]) -> str:
    sheet_url = st.get("sheet_url")
    tab = st.get("supplier_tab")
    tab_disp = html.escape(tab) if tab else "— ešte nie je nastavené v prostredí"
    link = ""
    if sheet_url:
        link = f'<a class="btn secondary" href="{html.escape(sheet_url)}" target="_blank" rel="noopener">Otvoriť Google Sheet</a>'
    else:
        link = "<p class=\"status-line\">Google Sheet: nastav <code>GOOGLE_SHEET_ID</code> na Verceli (raz pri spustení).</p>"

    body = f"""
    <h1>Prehľad</h1>
    <p class="lead">Tu nahráš export od dodávateľa a spustíš aktualizáciu reportu. Účty API sú v prostredí servera — ty používaš len túto stránku.</p>
    <p class="status-line">Záložka nákladov: <code>{tab_disp}</code></p>
    {link}
    <div class="cards" style="margin-top:1.25rem">
      <div class="card">
        <h2>Náklady z BillDetail</h2>
        <p>Súbor .xls / .xlsx z dodávateľa sa spracuje a doplní do záložky nákladov v tabuľke.</p>
        <a class="btn" href="/app/naklady">Nahrať súbor</a>
      </div>
      <div class="card">
        <h2>Report (Shopify + Meta → Sheet)</h2>
        <p>Spustí celý pipeline a obnoví dáta v reporte.</p>
        <form method="post" action="/app/report"><button type="submit" class="btn">Spustiť aktualizáciu</button></form>
      </div>
    </div>
    """
    return _shell(title="Domov", body=body, active="home")


def page_naklady() -> str:
    body = """
    <h1>Náklady dodávateľa</h1>
    <p class="lead">Vyber BillDetail export (.xls alebo .xlsx). Existujúce riadky v záložke nákladov sa nahradia týmto súborom.</p>
    <form method="post" action="/supplier-import" enctype="multipart/form-data">
      <label for="file">Súbor</label>
      <input type="file" id="file" name="file" accept=".xls,.xlsx,application/vnd.ms-excel" required/>
      <button type="submit" class="btn" style="margin-top:1rem;width:100%">Nahrať a zapísať do tabuľky</button>
    </form>
    <p class="hint">Max. cca 4 MB (limit hostingu).</p>
    """
    return _shell(title="Náklady dodávateľa", body=body, active="naklady")


def page_message(*, ok: bool, title: str, message: str, back_href: str) -> str:
    cls = "ok" if ok else "err"
    body = f"""
    <h1>{html.escape(title)}</h1>
    <pre class="msg {cls}">{html.escape(message)}</pre>
    <p class="back"><a href="{html.escape(back_href)}">← Späť</a></p>
    """
    return _shell(title=title, body=body, active=None)
