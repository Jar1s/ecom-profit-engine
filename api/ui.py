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
    name = os.getenv("GOOGLE_SHEET_NAME", "").strip()
    if os.getenv("SUPPLIER_COSTS_FROM_CSV", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        tab_disp = "CSV (pipeline číta súbor, nie Sheet)"
    else:
        tab_disp = os.getenv("SUPPLIER_COSTS_SHEET_TAB", "").strip() or "SUPPLIER_COSTS"
    return {
        "sheet_url": _sheet_url(),
        "supplier_tab": tab_disp,
        "sheet_title_hint": name or None,
        "cron_secret_set": bool(os.getenv("CRON_SECRET", "").strip()),
    }


def _fmt_cell(value: Any) -> str:
    if value is None:
        return "—"
    s = str(value).strip()
    return html.escape(s) if s else "—"


def _nav(active: str | None = None) -> str:
    items = [
        ("Dashboard", "/app", "home"),
        ("Orders", "/app/orders", "orders"),
        ("Daily", "/app/daily", "daily"),
        ("Marketing", "/app/marketing", "marketing"),
        ("Accounting", "/app/accounting", "accounting"),
        ("Payouts", "/app/payouts", "payouts"),
        ("Costs", "/app/costs", "costs"),
        ("Jobs", "/app/jobs", "jobs"),
    ]
    parts: list[str] = []
    for label, href, key in items:
        cls = ' class="active"' if active == key else ""
        parts.append(f'<a href="{html.escape(href)}"{cls}>{html.escape(label)}</a>')
    parts.append('<a href="/app/logout">Odhlásiť</a>')
    return "".join(parts)


def _shell(*, title: str, subtitle: str = "", body: str, active: str | None = None, show_nav: bool = True) -> str:
    nav_html = f'<nav class="nav">{_nav(active)}</nav>' if show_nav else ""
    subtitle_html = f'<p class="page-subtitle">{html.escape(subtitle)}</p>' if subtitle else ""
    return f"""<!DOCTYPE html>
<html lang="sk">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <meta name="robots" content="noindex,nofollow"/>
  <title>{html.escape(title)} — Ecom Profit</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
    :root {{
      --bg: #0b1018;
      --bg2: #111a26;
      --panel: rgba(18, 25, 38, 0.82);
      --panel-strong: #131d2b;
      --line: rgba(170, 197, 255, 0.13);
      --line-strong: rgba(126, 162, 255, 0.22);
      --text: #edf2ff;
      --muted: #9cabca;
      --soft: #6f7f9f;
      --blue: #67a4ff;
      --cyan: #76e1ff;
      --green: #4fe0a4;
      --red: #ff7d86;
      --amber: #ffc56a;
      --shadow: 0 24px 70px rgba(0,0,0,.34);
    }}
    * {{ box-sizing: border-box; }}
    html, body {{ margin: 0; min-height: 100%; }}
    body {{
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(103,164,255,.22), transparent 28%),
        radial-gradient(circle at top right, rgba(118,225,255,.14), transparent 25%),
        linear-gradient(180deg, #09111a 0%, #0b1018 100%);
    }}
    a {{ color: inherit; text-decoration: none; }}
    .wrap {{ max-width: 1380px; margin: 0 auto; padding: 28px 20px 60px; }}
    .brandbar {{ display:flex; align-items:end; justify-content:space-between; gap:16px; margin-bottom: 22px; }}
    .brand {{ font-size: 2rem; font-weight: 700; letter-spacing: -0.04em; }}
    .brand span {{ color: var(--blue); }}
    .tagline {{ color: var(--muted); max-width: 640px; font-size: .98rem; }}
    .nav {{
      display:flex; flex-wrap:wrap; gap:10px; margin: 0 0 22px; padding: 14px;
      border: 1px solid var(--line); border-radius: 20px; background: rgba(9,14,22,.5); backdrop-filter: blur(14px);
    }}
    .nav a {{ padding: 10px 14px; border-radius: 999px; color: var(--muted); font-size: .95rem; }}
    .nav a:hover {{ background: rgba(255,255,255,.04); color: var(--text); }}
    .nav a.active {{ background: linear-gradient(135deg, rgba(103,164,255,.18), rgba(118,225,255,.12)); color: var(--text); border: 1px solid var(--line-strong); }}
    .pagehead {{ display:flex; justify-content:space-between; gap:18px; align-items:end; margin-bottom: 18px; }}
    h1 {{ margin:0; font-size: 2.2rem; line-height:1; letter-spacing:-0.05em; }}
    .page-subtitle {{ margin: 10px 0 0; color: var(--muted); max-width: 800px; }}
    .statusbar {{ display:flex; gap:10px; flex-wrap:wrap; }}
    .pill {{ padding:8px 12px; border-radius:999px; background: rgba(255,255,255,.04); border:1px solid var(--line); color: var(--muted); font-size:.84rem; }}
    .grid {{ display:grid; gap:16px; }}
    .cards {{ grid-template-columns: repeat(4, minmax(0,1fr)); }}
    .two {{ grid-template-columns: 1.2fr .8fr; }}
    .panel {{ background: var(--panel); border:1px solid var(--line); border-radius: 24px; box-shadow: var(--shadow); overflow:hidden; }}
    .panel-head {{ padding: 18px 20px 10px; display:flex; justify-content:space-between; gap:12px; align-items:center; }}
    .panel-head h2 {{ margin:0; font-size: 1.05rem; }}
    .panel-head p {{ margin:4px 0 0; color: var(--muted); font-size:.9rem; }}
    .panel-body {{ padding: 0 20px 20px; }}
    .kpi {{ padding: 18px; min-height: 128px; position: relative; }}
    .kpi:before {{ content:""; position:absolute; inset:0; background: linear-gradient(135deg, rgba(103,164,255,.09), transparent 55%); pointer-events:none; }}
    .kpi-label {{ color: var(--muted); font-size: .88rem; margin-bottom: 14px; position:relative; }}
    .kpi-value {{ font-size: 1.85rem; font-weight: 700; letter-spacing: -0.05em; position:relative; }}
    .kpi-meta {{ color: var(--soft); font-size: .84rem; margin-top: 12px; position:relative; }}
    .action-row {{ display:flex; gap:10px; flex-wrap:wrap; }}
    .btn, button.btn {{
      display:inline-flex; align-items:center; justify-content:center; gap:8px; padding: 12px 16px; border-radius: 14px;
      background: linear-gradient(135deg, #4a85ff, #67a4ff); color:#fff; font-weight:700; border:none; cursor:pointer;
      box-shadow: inset 0 1px 0 rgba(255,255,255,.15);
    }}
    .btn.secondary, button.btn.secondary {{ background: rgba(255,255,255,.03); border:1px solid var(--line); color: var(--text); box-shadow:none; }}
    .btn.ghost, button.btn.ghost {{ background: transparent; border:1px dashed var(--line-strong); color: var(--muted); box-shadow:none; }}
    .stack {{ display:grid; gap:12px; }}
    .list {{ display:grid; gap:10px; }}
    .list-item {{ display:flex; justify-content:space-between; gap:12px; align-items:start; padding:14px 16px; border-radius:16px; background: rgba(255,255,255,.025); border:1px solid rgba(255,255,255,.04); }}
    .list-item strong {{ display:block; }}
    .list-item small {{ color: var(--muted); }}
    .mono, code, pre, table td.code {{ font-family: "IBM Plex Mono", monospace; }}
    .table-wrap {{ overflow:auto; border-radius:18px; border:1px solid rgba(255,255,255,.05); background: rgba(5,8,13,.26); }}
    table {{ width:100%; border-collapse: collapse; min-width: 760px; }}
    th, td {{ padding: 12px 14px; border-bottom:1px solid rgba(255,255,255,.06); text-align:left; vertical-align:top; font-size:.9rem; }}
    th {{ position: sticky; top: 0; background: rgba(14,20,31,.96); color: var(--muted); font-weight:600; backdrop-filter: blur(10px); }}
    td {{ color: var(--text); }}
    tr:hover td {{ background: rgba(255,255,255,.025); }}
    .empty {{ padding: 24px; border-radius: 16px; border:1px dashed var(--line-strong); color: var(--muted); background: rgba(255,255,255,.02); }}
    .lead {{ color: var(--muted); line-height:1.6; }}
    .split {{ display:grid; gap:16px; grid-template-columns: 1fr 1fr; }}
    .form-grid {{ display:grid; gap:12px; }}
    label {{ display:block; color: var(--muted); font-size: .84rem; margin-bottom: 6px; }}
    input[type="password"], input[type="file"], input[type="text"], select {{
      width:100%; padding: 12px 14px; border-radius: 14px; border:1px solid var(--line); background: rgba(8,12,18,.85); color: var(--text);
    }}
    .muted {{ color: var(--muted); }}
    .accent {{ color: var(--blue); }}
    .ok {{ color: var(--green); }}
    .err {{ color: var(--red); }}
    pre.msg {{ white-space: pre-wrap; background: rgba(8,12,18,.9); padding: 16px; border-radius: 16px; border: 1px solid var(--line); line-height:1.6; }}
    .inline-forms {{ display:flex; gap:10px; flex-wrap:wrap; }}
    @media (max-width: 1120px) {{
      .cards, .two, .split {{ grid-template-columns: 1fr 1fr; }}
    }}
    @media (max-width: 760px) {{
      .wrap {{ padding: 18px 14px 42px; }}
      .brandbar, .pagehead {{ display:block; }}
      .cards, .two, .split {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 1.75rem; }}
      .kpi-value {{ font-size: 1.55rem; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="brandbar">
      <div>
        <div class="brand">Ecom <span>Profit</span></div>
        <div class="tagline">Interný operating dashboard pre Shopify, Meta, supplier costs a tracking. Google Sheets ostáva zdroj pravdy, ale operatíva už nemusí žiť v sheetoch.</div>
      </div>
      <div class="statusbar">
        <div class="pill">/app internal</div>
        <div class="pill">Google Sheets backed</div>
      </div>
    </div>
    {nav_html}
    <div class="pagehead">
      <div>
        <h1>{html.escape(title)}</h1>
        {subtitle_html}
      </div>
    </div>
    {body}
  </div>
</body>
</html>"""


def _metric_cards(cards: list[dict[str, str]]) -> str:
    inner = "".join(
        f'''<div class="panel kpi"><div class="kpi-label">{_fmt_cell(c.get("label"))}</div><div class="kpi-value">{_fmt_cell(c.get("value"))}</div><div class="kpi-meta">{_fmt_cell(c.get("meta"))}</div></div>'''
        for c in cards
    )
    return f'<div class="grid cards">{inner}</div>' if inner else ""


def _table(df: Any, *, empty: str = "Zatiaľ bez dát.") -> str:
    if df is None or getattr(df, "empty", True):
        return f'<div class="empty">{html.escape(empty)}</div>'
    headers = [str(c) for c in df.columns]
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body_rows = []
    for _, row in df.iterrows():
        cells = "".join(f"<td>{_fmt_cell(row.get(h))}</td>" for h in headers)
        body_rows.append(f"<tr>{cells}</tr>")
    return f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{"".join(body_rows)}</tbody></table></div>'


def _list_rows(rows: list[dict[str, str]], left: tuple[str, str], right: tuple[str, str]) -> str:
    if not rows:
        return '<div class="empty">Zatiaľ bez záznamov.</div>'
    items = []
    for row in rows:
        items.append(
            f'''<div class="list-item"><div><strong>{_fmt_cell(row.get(left[0]))}</strong><small>{_fmt_cell(row.get(left[1]))}</small></div><div class="muted">{_fmt_cell(row.get(right[0]))}<br/><small>{_fmt_cell(row.get(right[1]))}</small></div></div>'''
        )
    return '<div class="list">' + "".join(items) + "</div>"


def page_login(*, error: str | None = None) -> str:
    err = f'<p class="err">{html.escape(error)}</p>' if error else ""
    body = f"""
    <div class="panel"><div class="panel-body" style="padding-top:20px">
      <p class="lead">Prihlásenie používa rovnaké heslo ako <code>CRON_SECRET</code>. Ak secret nemáš nastavený, login sa preskočí.</p>
      {err}
      <form method="post" action="/app/login" class="form-grid">
        <div>
          <label for="password">Heslo</label>
          <input type="password" id="password" name="password" autocomplete="current-password" required autofocus/>
        </div>
        <button type="submit" class="btn">Prihlásiť sa</button>
      </form>
    </div></div>
    """
    return _shell(title="Prihlásenie", subtitle="Bezpečný vstup do interného dashboardu.", body=body, show_nav=False)


def page_dashboard(*, status: dict[str, Any], cards: list[dict[str, str]], runs: list[dict[str, str]], recent_orders: Any, recent_daily: Any) -> str:
    sheet_cta = ""
    if status.get("sheet_url"):
        sheet_cta = f'<a class="btn secondary" href="{html.escape(str(status["sheet_url"]))}" target="_blank" rel="noopener">Otvoriť Google Sheet</a>'
    body = f"""
      {_metric_cards(cards)}
      <div class="grid two" style="margin-top:16px">
        <section class="panel">
          <div class="panel-head"><div><h2>Job Control</h2><p>Spúšťanie pipeline módov priamo z aplikácie.</p></div></div>
          <div class="panel-body stack">
            <div class="inline-forms">
              <form method="post" action="/app/run/core"><button class="btn" type="submit">Run Core</button></form>
              <form method="post" action="/app/run/tracking"><button class="btn secondary" type="submit">Run Tracking</button></form>
              <form method="post" action="/app/run/reporting"><button class="btn secondary" type="submit">Run Reporting</button></form>
              <form method="post" action="/app/run/full"><button class="btn ghost" type="submit">Full Rebuild</button></form>
            </div>
            <div class="stack">
              <div class="list-item"><div><strong>Supplier costs source</strong><small>{_fmt_cell(status.get('supplier_tab'))}</small></div><div>{sheet_cta or '<span class="muted">Doplň GOOGLE_SHEET_ID pre priamy odkaz.</span>'}</div></div>
              <div class="list-item"><div><strong>Web import</strong><small>BillDetail upload spustí import + pipeline podľa tvojho flow</small></div><div><a class="btn secondary" href="/app/costs">Open Costs</a></div></div>
            </div>
          </div>
        </section>
        <section class="panel">
          <div class="panel-head"><div><h2>Run Status</h2><p>Checkpointy a posledné úspešné joby zo <span class="mono">PIPELINE_STATE</span>.</p></div></div>
          <div class="panel-body">{_list_rows(runs, ('job','purpose'), ('last_run','purpose'))}</div>
        </section>
      </div>
      <div class="grid two" style="margin-top:16px">
        <section class="panel">
          <div class="panel-head"><div><h2>Recent Orders</h2><p>Posledné order-level riadky bez potreby otvárať ORDER_LEVEL v sheete.</p></div></div>
          <div class="panel-body">{_table(recent_orders, empty='ORDER_LEVEL zatiaľ nemá dáta.')}</div>
        </section>
        <section class="panel">
          <div class="panel-head"><div><h2>Recent Daily Summary</h2><p>Revenue, cost, profit a ad spend po dňoch.</p></div></div>
          <div class="panel-body">{_table(recent_daily, empty='DAILY_SUMMARY zatiaľ nemá dáta.')}</div>
        </section>
      </div>
    """
    return _shell(title="Dashboard", subtitle="Operatívny prehľad nad pipeline výstupmi, jobmi a poslednými výsledkami.", body=body, active="home")


def page_orders(table: Any) -> str:
    body = f'''<section class="panel"><div class="panel-head"><div><h2>ORDER_LEVEL</h2><p>Najnovšie objednávky, stav fulfillmentu, doručenie a profit na order úrovni.</p></div></div><div class="panel-body">{_table(table, empty='Žiadne order-level dáta.')}</div></section>'''
    return _shell(title="Orders", subtitle="Objednávky, fulfillment, tracking a profit bez preklikávania v Google Sheets.", body=body, active="orders")


def page_daily(table: Any) -> str:
    body = f'''<section class="panel"><div class="panel-head"><div><h2>DAILY_SUMMARY</h2><p>Denné P&L, orders delivered/undelivered a marketing spend.</p></div></div><div class="panel-body">{_table(table, empty='Žiadne daily summary dáta.')}</div></section>'''
    return _shell(title="Daily", subtitle="Denný performance pohľad na revenue, cost, profit a ROAS.", body=body, active="daily")


def page_marketing(meta_table: Any, campaign_table: Any) -> str:
    body = f'''
    <div class="grid two">
      <section class="panel"><div class="panel-head"><div><h2>META_DATA</h2><p>Denný Meta spend, ktorý feeduje DAILY_SUMMARY.</p></div></div><div class="panel-body">{_table(meta_table, empty='META_DATA zatiaľ bez dát.')}</div></section>
      <section class="panel"><div class="panel-head"><div><h2>META_CAMPAIGNS</h2><p>Campaign × day rozpad spendu a výkonu.</p></div></div><div class="panel-body">{_table(campaign_table, empty='META_CAMPAIGNS zatiaľ bez dát.')}</div></section>
    </div>
    '''
    return _shell(title="Marketing", subtitle="Denný spend aj campaign breakdown v jednom rozhraní.", body=body, active="marketing")


def page_accounting(table: Any) -> str:
    body = f'''<section class="panel"><div class="panel-head"><div><h2>BOOKKEEPING</h2><p>Manažérsky mesačný P&L z pipeline bez ručného čítania sheet tabu.</p></div></div><div class="panel-body">{_table(table, empty='BOOKKEEPING zatiaľ bez dát.')}</div></section>'''
    return _shell(title="Accounting", subtitle="Mesačný management P&L pre operating review.", body=body, active="accounting")


def page_payouts(table: Any) -> str:
    body = f'''<section class="panel"><div class="panel-head"><div><h2>PAYOUTS_FEES</h2><p>Shopify payout transakcie a fee náklady pre net profit.</p></div></div><div class="panel-body">{_table(table, empty='PAYOUTS_FEES zatiaľ bez dát.')}</div></section>'''
    return _shell(title="Payouts", subtitle="Detail payout fee transakcií započítaných do after-fees profitu.", body=body, active="payouts")


def page_costs(missing_table: Any) -> str:
    body = f'''
      <div class="grid two">
        <section class="panel">
          <div class="panel-head"><div><h2>BillDetail Import</h2><p>Upload supplier exportu bez otvárania Google Sheetu.</p></div></div>
          <div class="panel-body stack">
            <p class="lead">Vyber <strong>.xls</strong> alebo <strong>.xlsx</strong>. Import zapíše supplier costs a následne môže spustiť pipeline.</p>
            <form method="post" action="/supplier-import" enctype="multipart/form-data" class="form-grid">
              <div>
                <label for="file">Súbor</label>
                <input type="file" id="file" name="file" accept=".xls,.xlsx,application/vnd.ms-excel" required/>
              </div>
              <button type="submit" class="btn">Nahrať a aktualizovať</button>
            </form>
          </div>
        </section>
        <section class="panel">
          <div class="panel-head"><div><h2>Missing Supplier Costs</h2><p>Agregát chýbajúcich cost mappingov z <span class="mono">MISSING_SUPPLIER_COSTS</span>.</p></div></div>
          <div class="panel-body">{_table(missing_table, empty='Žiadne chýbajúce supplier costs.')}</div></section>
      </div>
    '''
    return _shell(title="Costs", subtitle="Supplier import aj quality control nákladov na jednom mieste.", body=body, active="costs")


def page_jobs(runs: list[dict[str, str]]) -> str:
    body = f'''
    <div class="grid two">
      <section class="panel"><div class="panel-head"><div><h2>Run Jobs</h2><p>Manuálne spúšťanie pipeline režimov priamo z UI.</p></div></div>
        <div class="panel-body stack">
          <div class="list-item"><div><strong>Core</strong><small>Shopify + supplier costs + daily Meta + hlavné taby</small></div><div><form method="post" action="/app/run/core"><button class="btn" type="submit">Spustiť</button></form></div></div>
          <div class="list-item"><div><strong>Tracking</strong><small>17TRACK + delivery refresh len pre aktívne zásielky</small></div><div><form method="post" action="/app/run/tracking"><button class="btn secondary" type="submit">Spustiť</button></form></div></div>
          <div class="list-item"><div><strong>Reporting</strong><small>META_CAMPAIGNS a BOOKKEEPING</small></div><div><form method="post" action="/app/run/reporting"><button class="btn secondary" type="submit">Spustiť</button></form></div></div>
          <div class="list-item"><div><strong>Full rebuild</strong><small>Fallback/debug run celého pipeline v jednom kroku</small></div><div><form method="post" action="/app/run/full"><button class="btn ghost" type="submit">Spustiť</button></form></div></div>
        </div>
      </section>
      <section class="panel"><div class="panel-head"><div><h2>Run History Snapshot</h2><p>To isté, čo drží <span class="mono">PIPELINE_STATE</span>.</p></div></div><div class="panel-body">{_list_rows(runs, ('job','purpose'), ('last_run','purpose'))}</div></section>
    </div>
    '''
    return _shell(title="Jobs", subtitle="Manuálna operatíva nad schedulerom a fallback runmi.", body=body, active="jobs")


def page_message(*, ok: bool, title: str, message: str, back_href: str) -> str:
    cls = "ok" if ok else "err"
    body = f"""
    <section class="panel"><div class="panel-body" style="padding-top:20px">
      <pre class="msg {cls}">{html.escape(message)}</pre>
      <p><a class="btn secondary" href="{html.escape(back_href)}">← Späť</a></p>
    </div></section>
    """
    return _shell(title=title, subtitle="Výsledok operácie v aplikácii.", body=body)
