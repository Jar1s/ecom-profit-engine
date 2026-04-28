# Dashboard (Next.js)

Internal `/app` UI: **Next.js 15** (App Router), **`output: 'export'`**, **`basePath: /app`**. Build copies `out/` → `../static/app/` for FastAPI.

## Dev (HMR + FastAPI API)

1. Run API locally (e.g. `uvicorn` on port **8000**).
2. `npm install` then `npm run dev` (port **5173**). `next.config.ts` rewrites `/api/*` and `/import-bill-detail` to `127.0.0.1:8000` only when `NODE_ENV=development`.

## Production build

From this directory: `npm run build` (runs `next build` + `scripts/export-to-static-app.mjs`).

## Executive traffic-light thresholds

Server env `DASHBOARD_EXECUTIVE_THRESHOLDS_JSON` (JSON object) merges partial overrides onto defaults in `api/dashboard.py` and is returned on `GET /api/app/dashboard` as `executive_thresholds`. Change on Vercel without rebuilding the static export.
