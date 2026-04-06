# Ecom Profit Engine (MVP)

Python pipeline: **Shopify** orders (line items) + **Meta Ads** daily spend + **supplier CSV** unit costs → **Google Sheets** with profit at line, order, and day level.

## Metrics naming

- **Product_Cost** — COGS from supplier CSV × quantity (unit cost per product name).
- **Gross_Profit** — `Revenue - Product_Cost` per line; rolled up to orders and days.
- **Ad_Spend** — Meta Marketing API insights, daily.
- **Marketing_ROAS** — `Revenue / Ad_Spend` for days where `Ad_Spend > 0`. This is account-level revenue over ad spend (sometimes called MER/blended ROAS). It is **not** `Revenue / Product_Cost`.

Order dates use Shopify `created_at` (ISO UTC). For store-local “business days”, align reporting timezone in a later phase.

## Setup

1. Python 3.10+ recommended.
2. `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill secrets.
4. Google Sheets: either put a service account JSON file on disk (`GOOGLE_CREDS`, default `creds.json`) **or** set `GOOGLE_CREDENTIALS_JSON` to the full JSON string (used on Vercel / CI). Share the spreadsheet with the service account email (Editor).
5. Put supplier costs in `data/supplier_costs.csv` (`Product`, `Cost` columns). Product names are matched after **trim + case-insensitive** normalization; tune titles in CSV to match Shopify line item names.

## Environment variables

See `.env.example`. Required: `SHOPIFY_STORE`, `SHOPIFY_TOKEN`, `META_TOKEN`, `AD_ACCOUNT_ID`, `GOOGLE_SHEET_NAME`. For Google auth, set **`GOOGLE_CREDENTIALS_JSON`** (recommended for hosted runs) or **`GOOGLE_CREDS`** path to a JSON file.

Meta date range: either `META_LOOKBACK_DAYS` (default 90, ending today) or both `META_TIME_RANGE_SINCE` and `META_TIME_RANGE_UNTIL` (`YYYY-MM-DD`).

Optional worksheet names: `SHEET_TAB_ORDERS_DB`, `SHEET_TAB_ORDER_LEVEL`, `SHEET_TAB_META_DATA`, `SHEET_TAB_DAILY_SUMMARY` (defaults: `ORDERS_DB`, `ORDER_LEVEL`, `META_DATA`, `DAILY_SUMMARY`).

## Run

```bash
cd ecom-profit-engine
python pipeline.py
```

(Do **not** add a root `main.py` — Vercel reserves that name as a Python web entrypoint and your `api/*.py` routes may not deploy as Serverless Functions.)

Exit code `0` on success, `1` on configuration or fatal errors.

## Automation

### Cron (macOS/Linux)

Run every 2 hours (adjust path):

```cron
0 */2 * * * cd /path/to/ecom-profit-engine && /usr/bin/python3 pipeline.py >> /var/log/ecom-profit.log 2>&1
```

### GitHub Actions

Use a scheduled workflow with repository secrets:

- `SHOPIFY_STORE`, `SHOPIFY_TOKEN`, `META_TOKEN`, `AD_ACCOUNT_ID`, `GOOGLE_SHEET_NAME`
- `GOOGLE_CREDENTIALS_JSON` — **full** service account JSON string (secret)

The workflow passes `GOOGLE_CREDENTIALS_JSON` directly into the process (no file on disk). See [.github/workflows/ecom-profit-engine.yml](.github/workflows/ecom-profit-engine.yml).

### Vercel (cron + serverless)

If this repository is only this project, use the repo root as the Vercel **Root Directory** (default). If the app lives inside a monorepo subfolder, set **Root Directory** to `ecom-profit-engine`.

1. **Import** the project in [Vercel](https://vercel.com).
2. **Environment variables** (Production): same as local, plus:
   - `GOOGLE_CREDENTIALS_JSON` — entire service account JSON (one line or multiline in the dashboard).
   - `CRON_SECRET` — random string; Vercel Cron will send `Authorization: Bearer <CRON_SECRET>` to `/api/cron` (implemented in [`app.py`](app.py) via FastAPI). Without `CRON_SECRET`, the endpoint is open (fine for local tests only).
3. **`vercel.json`** sets `maxDuration` for `app.py` (the single Python Serverless Function). On Vercel, **Hobby** allows up to **300s** per function; **Pro** (paid plan) allows up to **800s** if you need longer runs. If you hit `FUNCTION_INVOCATION_TIMEOUT`, raise `maxDuration` within your plan’s cap or reduce work (e.g. `META_LOOKBACK_DAYS`, fewer orders per run).
4. After deploy, cron runs automatically. You can also trigger **GET** or **POST** `https://<your-deployment>/api/cron` with the `Authorization` header when `CRON_SECRET` is set.

`supplier_costs.csv` is read from the deployment bundle (commit it or adjust `SUPPLIER_COSTS_CSV` if you change layout).

## Shopify API

- Custom app in Shopify Admin → API access token with `read_orders` (and related scopes for orders).
- Pagination uses `Link` / `rel="next"` cursors (250 per page).

## Meta Marketing API

- System user or app with `ads_read`; long-lived access token.
- On `401` / OAuth `190`, refresh the token and update `META_TOKEN`.

## Tests

```bash
cd ecom-profit-engine
python -m unittest discover -s tests -p 'test_*.py' -v
```

## Limitations (MVP)

- Line revenue is `price × quantity`; discounts are not allocated line-by-line.
- Refunds: not modeled (future phase).
- Product name matching is normalized text only; use aliases in a future iteration if needed.
