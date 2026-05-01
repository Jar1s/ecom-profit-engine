"use client";

import { useMutation, useQuery } from "@tanstack/react-query";
import { ExternalLink, Loader2, Play } from "lucide-react";
import { apiGet, apiPostJson } from "@/lib/api";
import { buildDailyTrendPoints } from "@/lib/chart-data";
import { mergeExecutiveThresholds } from "@/lib/executive-thresholds";
import type { DashboardResponse, PipelineRunResult } from "@/types";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DataTable } from "@/components/data-table";
import { DashboardTrends } from "@/components/dashboard-trends";

function num(v: unknown): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const n = Number(String(v).replace(/\s/g, "").replace(",", "."));
    return Number.isFinite(n) ? n : 0;
  }
  return 0;
}

function fmtMoney(v: number): string {
  return `${v.toLocaleString("sk-SK", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function fmtPct(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return `${v.toFixed(1)}%`;
}

function fmtInt(v: number): string {
  return `${Math.round(v).toLocaleString("sk-SK")}`;
}

type Level = "green" | "amber" | "red" | "na";

function trendBadge(deltaPct: number | null) {
  if (deltaPct == null || !Number.isFinite(deltaPct)) {
    return <span className="rounded-full border border-slate-200 px-2 py-0.5 text-[11px] text-slate-500">n/a</span>;
  }
  const up = deltaPct >= 0;
  return (
    <span
      className={`rounded-full px-2 py-0.5 text-[11px] font-semibold ${
        up ? "bg-emerald-50 text-emerald-700" : "bg-rose-50 text-rose-700"
      }`}
    >
      {up ? "+" : ""}
      {deltaPct.toFixed(1)}%
    </span>
  );
}

function MiniSpark({ points }: { points: number[] }) {
  const max = Math.max(...points, 1);
  return (
    <div className="mt-2 flex h-8 items-end gap-1">
      {points.map((p, idx) => (
        <span
          // sparkline bars are intentionally index-keyed from sorted fixed window
          key={idx}
          className="w-1.5 rounded-sm bg-slate-300"
          style={{ height: `${Math.max(8, (p / max) * 100)}%` }}
        />
      ))}
    </div>
  );
}

function levelPill(level: Level) {
  const map: Record<Level, { text: string; cls: string }> = {
    green: { text: "GREEN", cls: "bg-emerald-100 text-emerald-800" },
    amber: { text: "AMBER", cls: "bg-amber-100 text-amber-800" },
    red: { text: "RED", cls: "bg-rose-100 text-rose-800" },
    na: { text: "N/A", cls: "bg-slate-100 text-slate-500" },
  };
  const v = map[level];
  return <span className={`rounded-full px-2 py-0.5 text-[11px] font-semibold ${v.cls}`}>{v.text}</span>;
}

function evaluateThreshold(
  value: number | null,
  cfg: { mode: "higher_better" | "lower_better"; green: number; amber: number },
): Level {
  if (value == null || !Number.isFinite(value)) return "na";
  if (cfg.mode === "higher_better") {
    if (value >= cfg.green) return "green";
    if (value >= cfg.amber) return "amber";
    return "red";
  }
  if (value <= cfg.green) return "green";
  if (value <= cfg.amber) return "amber";
  return "red";
}

function KpiGrid({
  cards,
  trendByLabel,
  sparkByLabel,
  levelByLabel,
}: {
  cards: DashboardResponse["cards"];
  trendByLabel: Record<string, number | null>;
  sparkByLabel: Record<string, number[]>;
  levelByLabel: Record<string, Level>;
}) {
  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      {cards.map((c) => (
        <Card key={c.label} className="overflow-hidden">
          <CardContent className="p-5">
            <div className="flex items-center justify-between gap-2">
              <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">{c.label}</p>
              <div className="flex items-center gap-2">
                {levelPill(levelByLabel[c.label] ?? "na")}
                {trendBadge(trendByLabel[c.label] ?? null)}
              </div>
            </div>
            <p className="mt-2 text-2xl font-bold tabular-nums tracking-tight text-slate-900 sm:text-[1.65rem]">{c.value}</p>
            {sparkByLabel[c.label]?.length ? <MiniSpark points={sparkByLabel[c.label]} /> : null}
            <p className="mt-2 text-xs leading-snug text-slate-600">{c.meta}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function IndicatorGrid({
  rows,
  trendByLabel,
  levelByLabel,
}: {
  rows: Array<{ label: string; value: string; meta: string }>;
  trendByLabel: Record<string, number | null>;
  levelByLabel: Record<string, Level>;
}) {
  return (
    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      {rows.map((row) => (
        <Card key={row.label}>
          <CardContent className="p-4">
            <div className="flex items-center justify-between gap-2">
              <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{row.label}</p>
              <div className="flex items-center gap-2">
                {levelPill(levelByLabel[row.label] ?? "na")}
                {trendBadge(trendByLabel[row.label] ?? null)}
              </div>
            </div>
            <p className="mt-1 text-xl font-bold tracking-tight text-slate-900">{row.value}</p>
            <p className="mt-1 text-xs text-slate-600">{row.meta}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function RunBlock({ runs }: { runs: DashboardResponse["runs"] }) {
  return (
    <div className="space-y-2">
      {runs.map((r, idx) => {
        const isErr = r.job === "last_error";
        return (
          <div
            key={`${idx}-${r.job}`}
            className={`flex flex-col gap-1 rounded-lg border px-4 py-3 sm:flex-row sm:items-start sm:justify-between ${
              isErr ? "border-red-200 bg-red-50" : "border-slate-200 bg-slate-50/60"
            }`}
          >
            <div>
              <p className="font-mono text-sm font-semibold text-slate-900">{r.job}</p>
              <p className="mt-1 text-xs text-slate-600">{r.purpose}</p>
            </div>
            <p className={`shrink-0 font-mono text-xs sm:text-right ${isErr ? "text-red-700" : "text-slate-500"}`}>
              {r.last_run}
            </p>
          </div>
        );
      })}
    </div>
  );
}

export function DashboardHome() {
  const q = useQuery({
    queryKey: ["dashboard"],
    queryFn: () => apiGet<DashboardResponse>("/dashboard"),
  });

  const runMut = useMutation({
    mutationFn: (mode: string) => apiPostJson<PipelineRunResult>(`/run/${mode}`),
    onSettled: () => {
      void q.refetch();
    },
  });

  if (q.error && !q.data) {
    return <p className="text-sm font-medium text-red-700">{q.error.message}</p>;
  }

  if (q.isPending || !q.data) {
    return (
      <div className="flex items-center gap-2 text-slate-600">
        <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
        Načítavam dáta…
      </div>
    );
  }

  const data = q.data;
  const st = data.status;
  const chartPoints = buildDailyTrendPoints(data.recent_daily, 30);
  const daily = data.recent_daily;
  const ordersRows = data.recent_orders;
  const dailySorted = [...daily].sort((a, b) => String(a.Date ?? "").localeCompare(String(b.Date ?? "")));
  const last7 = dailySorted.slice(-7);
  const prev7 = dailySorted.slice(-14, -7);
  const sumField = (rows: Record<string, unknown>[], key: string) => rows.reduce((acc, r) => acc + num(r[key]), 0);
  const pctDelta = (cur: number, prev: number) => (prev > 0 ? ((cur - prev) / prev) * 100 : null);
  const avgField = (rows: Record<string, unknown>[], key: string) => (rows.length ? sumField(rows, key) / rows.length : 0);

  const revenue30 = daily.reduce((acc, row) => acc + num(row.Revenue), 0);
  const gross30 = daily.reduce((acc, row) => acc + num(row.Gross_Profit), 0);
  const ad30 = daily.reduce((acc, row) => acc + num(row.Ad_Spend), 0);
  const orders30 = daily.reduce((acc, row) => acc + num(row.Orders_Total), 0);
  const delivered30 = daily.reduce((acc, row) => acc + num(row.Orders_Delivered), 0);
  const undelivered30 = daily.reduce((acc, row) => acc + num(row.Orders_Undelivered), 0);
  const grossMargin = revenue30 > 0 ? (gross30 / revenue30) * 100 : null;
  const adShare = revenue30 > 0 ? (ad30 / revenue30) * 100 : null;
  const aov = orders30 > 0 ? revenue30 / orders30 : null;
  const deliveryRate = orders30 > 0 ? (delivered30 / orders30) * 100 : null;
  const profitAfterAds = gross30 - ad30;
  const undeliveredNow = ordersRows.filter(
    (row) => String(row.Delivery_Status ?? "").trim().toLowerCase() !== "delivered",
  ).length;
  const avgTransit = (() => {
    const vals = ordersRows
      .map((r) => num(r.Days_In_Transit))
      .filter((v) => Number.isFinite(v) && v > 0);
    if (!vals.length) return null;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  })();
  const lastSyncCard = data.cards.find((c) => c.label === "Last Sync");
  const healthIndicators = [
    { label: "Gross Margin", value: fmtPct(grossMargin), meta: "Gross Profit / Revenue (30d)" },
    { label: "Ad Share", value: fmtPct(adShare), meta: "Ad Spend / Revenue (30d)" },
    { label: "AOV", value: aov == null ? "—" : fmtMoney(aov), meta: "Average order value (30d)" },
    { label: "Delivery Rate", value: fmtPct(deliveryRate), meta: "Delivered / Orders (30d)" },
    { label: "Profit After Ads", value: fmtMoney(profitAfterAds), meta: "Gross Profit - Ad Spend (30d)" },
    { label: "Undelivered Now", value: fmtInt(undeliveredNow), meta: "Aktívne nedoručené objednávky" },
    { label: "Undelivered 30d", value: fmtInt(undelivered30), meta: "Súčet denných nedoručených za 30d" },
    { label: "Avg Transit Days", value: avgTransit == null ? "—" : `${avgTransit.toFixed(1)} d`, meta: "Priemer z recent orders" },
  ];
  const thresholdValueByLabel: Record<string, number | null> = {
    "Revenue 30d": revenue30,
    "Gross Profit 30d": gross30,
    "Ad Spend 30d": ad30,
    "Payout Fees 30d": null,
    "ROAS 30d": ad30 > 0 ? revenue30 / ad30 : null,
    "Orders 30d": orders30,
    Undelivered: undeliveredNow,
    "Operating Income": null,
    "Gross Margin": grossMargin,
    "Ad Share": adShare,
    AOV: aov,
    "Delivery Rate": deliveryRate,
    "Profit After Ads": profitAfterAds,
    "Profit After Fees": null,
    "Undelivered Now": undeliveredNow,
    "Undelivered 30d": undelivered30,
    "Avg Transit Days": avgTransit,
  };
  const thresholdCfgByLabel = mergeExecutiveThresholds(data.executive_thresholds);
  const levelByLabel: Record<string, Level> = Object.fromEntries(
    Object.entries(thresholdValueByLabel).map(([label, value]) => {
      const cfg = thresholdCfgByLabel[label];
      return [label, cfg ? evaluateThreshold(value, cfg) : "na"];
    }),
  ) as Record<string, Level>;
  const trendByLabel: Record<string, number | null> = {
    "Revenue 30d": pctDelta(sumField(last7, "Revenue"), sumField(prev7, "Revenue")),
    "Gross Profit 30d": pctDelta(sumField(last7, "Gross_Profit"), sumField(prev7, "Gross_Profit")),
    "Ad Spend 30d": pctDelta(sumField(last7, "Ad_Spend"), sumField(prev7, "Ad_Spend")),
    "Payout Fees 30d": null,
    "ROAS 30d": pctDelta(avgField(last7, "Marketing_ROAS"), avgField(prev7, "Marketing_ROAS")),
    "Orders 30d": pctDelta(sumField(last7, "Orders_Total"), sumField(prev7, "Orders_Total")),
    Undelivered: pctDelta(sumField(last7, "Orders_Undelivered"), sumField(prev7, "Orders_Undelivered")),
    "Operating Income": null,
    "Gross Margin": pctDelta(
      sumField(last7, "Revenue") > 0 ? (sumField(last7, "Gross_Profit") / sumField(last7, "Revenue")) * 100 : 0,
      sumField(prev7, "Revenue") > 0 ? (sumField(prev7, "Gross_Profit") / sumField(prev7, "Revenue")) * 100 : 0,
    ),
    "Ad Share": pctDelta(
      sumField(last7, "Revenue") > 0 ? (sumField(last7, "Ad_Spend") / sumField(last7, "Revenue")) * 100 : 0,
      sumField(prev7, "Revenue") > 0 ? (sumField(prev7, "Ad_Spend") / sumField(prev7, "Revenue")) * 100 : 0,
    ),
    AOV: pctDelta(
      sumField(last7, "Orders_Total") > 0 ? sumField(last7, "Revenue") / sumField(last7, "Orders_Total") : 0,
      sumField(prev7, "Orders_Total") > 0 ? sumField(prev7, "Revenue") / sumField(prev7, "Orders_Total") : 0,
    ),
    "Delivery Rate": pctDelta(
      sumField(last7, "Orders_Total") > 0 ? (sumField(last7, "Orders_Delivered") / sumField(last7, "Orders_Total")) * 100 : 0,
      sumField(prev7, "Orders_Total") > 0
        ? (sumField(prev7, "Orders_Delivered") / sumField(prev7, "Orders_Total")) * 100
        : 0,
    ),
    "Profit After Ads": pctDelta(sumField(last7, "Gross_Profit") - sumField(last7, "Ad_Spend"), sumField(prev7, "Gross_Profit") - sumField(prev7, "Ad_Spend")),
    "Profit After Fees": null,
    "Undelivered Now": null,
    "Undelivered 30d": pctDelta(sumField(last7, "Orders_Undelivered"), sumField(prev7, "Orders_Undelivered")),
    "Avg Transit Days": null,
  };
  const sparkByLabel: Record<string, number[]> = {
    "Revenue 30d": dailySorted.slice(-12).map((r) => num(r.Revenue)),
    "Gross Profit 30d": dailySorted.slice(-12).map((r) => num(r.Gross_Profit)),
    "Ad Spend 30d": dailySorted.slice(-12).map((r) => num(r.Ad_Spend)),
    "Payout Fees 30d": [],
    "ROAS 30d": dailySorted.slice(-12).map((r) => num(r.Marketing_ROAS)),
    "Orders 30d": dailySorted.slice(-12).map((r) => num(r.Orders_Total)),
    Undelivered: dailySorted.slice(-12).map((r) => num(r.Orders_Undelivered)),
    "Operating Income": [],
    "Profit After Fees": [],
  };

  const sheetBtn = st.sheet_url ? (
    <a
      href={st.sheet_url}
      target="_blank"
      rel="noopener noreferrer"
      className="inline-flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-900 shadow-sm hover:bg-slate-50"
    >
      Otvoriť Google Sheet
      <ExternalLink className="h-4 w-4 opacity-70" aria-hidden />
    </a>
  ) : (
    <span className="text-sm text-slate-600">Doplň GOOGLE_SHEET_ID pre priamy odkaz.</span>
  );

  return (
    <div className="space-y-10">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight text-slate-900">Dashboard</h1>
        <p className="max-w-3xl text-slate-600">
          Operatívny prehľad nad pipeline výstupmi, jobmi a poslednými výsledkami. Google Sheets ostáva zdroj pravdy.
        </p>
      </header>

      <div className="grid gap-4 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardContent className="flex flex-col gap-2 p-5 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Last Sync</p>
              <p className="text-xl font-bold tracking-tight text-slate-900">{lastSyncCard?.value ?? "—"}</p>
              <p className="text-xs text-slate-600">{lastSyncCard?.meta ?? "bez state"}</p>
            </div>
            <div className="flex flex-wrap items-center gap-2 text-xs">
              <span className="rounded-full border border-slate-200 bg-white px-3 py-1 text-slate-600">Dáta: 30d window</span>
              <span className="rounded-full border border-slate-200 bg-white px-3 py-1 text-slate-600">Source: Google Sheets</span>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-5">
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Orders Flow</p>
            <p className="mt-1 text-xl font-bold text-slate-900">{fmtInt(orders30)} / 30d</p>
            <p className="text-xs text-slate-600">Delivered {fmtInt(delivered30)} · Undelivered {fmtInt(undelivered30)}</p>
          </CardContent>
        </Card>
      </div>

      <KpiGrid
        cards={data.cards.filter((c) => c.label !== "Last Sync")}
        trendByLabel={trendByLabel}
        sparkByLabel={sparkByLabel}
        levelByLabel={levelByLabel}
      />

      <section className="space-y-3">
        <div>
          <h2 className="text-lg font-semibold tracking-tight text-slate-900">Operational indicators</h2>
          <p className="text-sm text-slate-600">Viac metrík pre rýchle rozhodovanie bez otvorenia sheetu.</p>
        </div>
        <IndicatorGrid rows={healthIndicators} trendByLabel={trendByLabel} levelByLabel={levelByLabel} />
        <Card>
          <CardContent className="p-4 text-xs text-slate-600">
            <p className="font-semibold text-slate-700">Executive threshold policy</p>
            <p className="mt-1">
              Prahy sú zámerne konzervatívne (3-level model Green/Amber/Red), aby sa predišlo alert fatigue. Ak metrika
              nemá stabilný benchmark (napr. Revenue absolútna hodnota), zobrazuje sa iba trend bez farby. Prahy vieš
              meniť cez env <span className="font-mono">DASHBOARD_EXECUTIVE_THRESHOLDS_JSON</span> na serveri (bez
              rebuildu frontendu).
            </p>
          </CardContent>
        </Card>
      </section>

      <DashboardTrends points={chartPoints} />

      <div className="grid gap-6 lg:grid-cols-5">
        <section className="lg:col-span-3">
          <Card>
            <CardHeader>
              <CardTitle>Job control</CardTitle>
              <CardDescription>Spúšťanie pipeline módov priamo z aplikácie.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="flex flex-wrap gap-3">
                {(
                  [
                    ["business", "primary", "Run Business"],
                    ["core", "primary", "Run Core"],
                    ["tracking", "secondary", "Run Tracking"],
                    ["reporting", "secondary", "Run Reporting"],
                    ["full", "ghost", "Full Rebuild"],
                  ] as const
                ).map(([mode, kind, label]) => (
                  <Button
                    key={mode}
                    variant={kind === "primary" ? "primary" : kind === "secondary" ? "secondary" : "ghost"}
                    disabled={!!runMut.isPending}
                    onClick={() => runMut.mutate(mode)}
                  >
                    {runMut.isPending && runMut.variables === mode ? (
                      <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
                    ) : (
                      <Play className="h-4 w-4 opacity-80" aria-hidden />
                    )}
                    {label}
                  </Button>
                ))}
              </div>
              {runMut.data || runMut.error ? (
                <p className="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 font-mono text-sm text-slate-700">
                  {runMut.error instanceof Error
                    ? runMut.error.message
                    : runMut.data?.message ||
                      (runMut.data?.ok ? "OK" : "Zlyhalo")}
                </p>
              ) : null}

              <div className="space-y-4 border-t border-slate-100 pt-6">
                <div className="flex flex-col justify-between gap-3 rounded-lg border border-slate-100 bg-slate-50/50 px-4 py-3 sm:flex-row sm:items-center">
                  <div>
                    <p className="text-sm font-semibold text-slate-900">Supplier costs source</p>
                    <p className="text-sm text-slate-600">{st.supplier_tab}</p>
                  </div>
                  {sheetBtn}
                </div>
                <div className="flex flex-col justify-between gap-3 rounded-lg border border-slate-100 bg-slate-50/50 px-4 py-3 sm:flex-row sm:items-center">
                  <div>
                    <p className="text-sm font-semibold text-slate-900">Web import</p>
                    <p className="text-sm text-slate-600">BillDetail upload — Costs tab.</p>
                  </div>
                  <a
                    href="/app/costs"
                    className="inline-flex shrink-0 items-center justify-center rounded-lg border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-900 shadow-sm hover:bg-slate-50"
                  >
                    Open Costs
                  </a>
                </div>
              </div>
            </CardContent>
          </Card>
        </section>

        <section className="lg:col-span-2">
          <Card className="h-full">
            <CardHeader>
              <CardTitle>Run status</CardTitle>
              <CardDescription>Checkpointy a posledné joby z PIPELINE_STATE.</CardDescription>
            </CardHeader>
            <CardContent>
              <RunBlock runs={data.runs} />
            </CardContent>
          </Card>
        </section>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Recent orders</CardTitle>
            <CardDescription>Posledné order-level riadky.</CardDescription>
          </CardHeader>
          <CardContent>
            <DataTable
              rows={data.recent_orders}
              empty="ORDER_LEVEL zatiaľ nemá dáta."
              monoCols={new Set(["Order_ID", "Tracking_Numbers", "Order"])}
            />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Recent daily summary</CardTitle>
            <CardDescription>Revenue, cost, profit a ad spend po dňoch.</CardDescription>
          </CardHeader>
          <CardContent>
            <DataTable rows={data.recent_daily} empty="DAILY_SUMMARY zatiaľ nemá dáta." />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
