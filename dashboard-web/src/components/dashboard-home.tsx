"use client";

import { useMutation, useQuery } from "@tanstack/react-query";
import { ExternalLink, Loader2, Play } from "lucide-react";
import { apiGet, apiPostJson } from "@/lib/api";
import { buildDailyTrendPoints } from "@/lib/chart-data";
import type { DashboardResponse, PipelineRunResult } from "@/types";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DataTable } from "@/components/data-table";
import { DashboardTrends } from "@/components/dashboard-trends";

function KpiGrid({ cards }: { cards: DashboardResponse["cards"] }) {
  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      {cards.map((c) => (
        <Card key={c.label} className="overflow-hidden">
          <CardContent className="p-5">
            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">{c.label}</p>
            <p className="mt-2 text-2xl font-bold tabular-nums tracking-tight text-slate-900 sm:text-[1.65rem]">{c.value}</p>
            <p className="mt-2 text-xs leading-snug text-slate-600">{c.meta}</p>
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

      <KpiGrid cards={data.cards} />

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
